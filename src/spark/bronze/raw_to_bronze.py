"""
Spark/Python Job: Raw Canonical Staging -> Bronze Iceberg Table.
Appends normalized multi-exchange records into lakehouse.bronze.crypto_bars
using PyIceberg streaming symbol-level append for maximum throughput and zero JVM heap pressure.
In accordance with PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
import sys
import time
import uuid

import pyarrow as pa
import pyarrow.dataset as ds
from pyiceberg.catalog import load_catalog


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Raw to Bronze Iceberg Ingestion")
    parser.add_argument(
        "--input-dir",
        default=None,
        help="Path to canonical staging directory",
    )
    parser.add_argument(
        "--target-table",
        default="bronze.crypto_bars",
        help="Target Iceberg Bronze table name (e.g. bronze.crypto_bars)",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional batch filter",
    )
    parser.add_argument(
        "--force-replay",
        action="store_true",
        help="Force re-ingestion of already processed files (bypasses manifest check)",
    )
    return parser.parse_args()


def resolve_input_path(custom_path: str | None) -> str:
    if custom_path:
        return custom_path
    if os.path.exists("/home/iceberg/warehouse_local/staging/canonical"):
        return "/home/iceberg/warehouse_local/staging/canonical"
    if os.path.exists("E:/project_hybrid_lakehouse_crypto/warehouse/staging/canonical"):
        return "E:/project_hybrid_lakehouse_crypto/warehouse/staging/canonical"
    if os.path.exists("E:/project_lakehouse_crypto/warehouse/staging/canonical"):
        return "E:/project_lakehouse_crypto/warehouse/staging/canonical"
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../../warehouse/staging/canonical",
        )
    )


def get_catalog():
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
    s3_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

    return load_catalog(
        "lakehouse",
        **{
            "type": "rest",
            "uri": rest_uri,
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key,
        },
    )


def main() -> None:
    args = parse_args()
    input_path = resolve_input_path(args.input_dir)
    target_table_name = args.target_table
    if target_table_name.startswith("lakehouse."):
        target_table_name = target_table_name[len("lakehouse."):]
    batch_id = args.batch_id or str(uuid.uuid4())

    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Raw -> Bronze ingestion...")
    print(f"Reading from: {input_path}")
    print(f"Writing to target table: {target_table_name}")

    catalog = get_catalog()
    table = catalog.load_table(target_table_name)

    total_ingested = 0
    t_start = time.time()

    exchange_dirs = [
        d for d in sorted(os.listdir(input_path))
        if os.path.isdir(os.path.join(input_path, d))
    ]
    if not exchange_dirs:
        exchange_dirs = ["."]

    for exc in exchange_dirs:
        exc_path = os.path.join(input_path, exc) if exc != "." else input_path
        symbol_dirs = sorted([
            s for s in os.listdir(exc_path)
            if os.path.isdir(os.path.join(exc_path, s))
        ])

        print(f"\n[{datetime.now(timezone.utc).isoformat()}] Found {len(symbol_dirs)} symbols for exchange '{exc}' in {exc_path}")

        for idx, sym in enumerate(symbol_dirs, 1):
            sym_dir = os.path.join(exc_path, sym)
            t0 = time.time()

            # Find matching parquet files
            parquet_files = [
                os.path.join(sym_dir, f) for f in os.listdir(sym_dir)
                if f.endswith(".parquet") and (not args.batch_id or args.batch_id[:8] in f)
            ]

            if not parquet_files:
                continue

            from src.utils.manifest_tracker import (
                compute_file_sha256,
                is_file_already_ingested,
                record_manifest_entry,
            )

            sym_ingested = 0
            sym_skipped = 0

            for p_file in parquet_files:
                file_name = os.path.basename(p_file)
                f_hash = compute_file_sha256(p_file) if os.path.exists(p_file) else None
                if not f_hash:
                    continue

                # Idempotency Gate: Skip already ingested files based on SHA-256 content hash
                if not args.force_replay and is_file_already_ingested(catalog, p_file, f_hash):
                    sym_skipped += 1
                    continue

                import pyarrow.parquet as pq
                file_tbl = pq.read_table(p_file)
                rows_count = len(file_tbl)
                if rows_count == 0:
                    continue

                now_utc = datetime.now(timezone.utc)
                file_tbl = file_tbl.append_column("ingested_at", pa.repeat(now_utc, rows_count))

                # PyIceberg streaming append to Bronze
                table.append(file_tbl)
                total_ingested += rows_count
                sym_ingested += rows_count

                # Record granular checkpoint for THIS EXACT file in bronze.source_manifest
                try:
                    min_ts = file_tbl["timestamp"][0].as_py() if rows_count > 0 else None
                    max_ts = file_tbl["timestamp"][-1].as_py() if rows_count > 0 else None
                    record_manifest_entry(
                        catalog=catalog,
                        source_file=p_file,
                        exchange=exc,
                        raw_symbol=sym,
                        batch_id=batch_id,
                        file_hash=f_hash,
                        record_count=rows_count,
                        min_timestamp=min_ts,
                        max_timestamp=max_ts,
                        status="INGESTED_BRONZE",
                    )
                except Exception as mf_err:
                    print(f"Warning: Failed to record manifest for {file_name}: {mf_err}")

            elapsed = time.time() - t0
            skip_msg = f" (skipped {sym_skipped} existing files)" if sym_skipped > 0 else ""
            print(f"[{exc} {idx}/{len(symbol_dirs)}] Ingested {sym_ingested:,} rows for {sym} in {elapsed:.2f}s{skip_msg} (running total: {total_ingested:,})")

    total_elapsed = time.time() - t_start
    print(f"\n[{datetime.now(timezone.utc).isoformat()}] Successfully ingested {total_ingested:,} total Bronze records in {total_elapsed:.1f}s ({total_ingested / max(total_elapsed, 1):,.0f} rows/s).")

    # Record pipeline audit
    if total_ingested > 0:
        try:
            audit_table = catalog.load_table("gold.pipeline_audit")
            audit_record = pa.Table.from_pydict({
                "run_id": [batch_id],
                "dag_id": ["lakehouse_historical_backfill"],
                "step_name": ["raw_to_bronze"],
                "status": ["SUCCESS"],
                "records_processed": [pa.scalar(total_ingested, type=pa.int64())],
                "details": [f"Appended {total_ingested:,} multi-exchange records to Bronze in {total_elapsed:.1f}s"],
                "created_at": [datetime.now(timezone.utc)],
            })
            audit_table.append(audit_record)
            print("Recorded Raw -> Bronze SUCCESS in gold.pipeline_audit.")
        except Exception as e:
            print(f"Warning: Failed to record audit log: {e}")


if __name__ == "__main__":
    main()

