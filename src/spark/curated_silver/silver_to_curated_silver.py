"""
Spark/Python Job: Silver Table -> Curated Silver Table (Intra-Exchange Priority Reconciliation).
Executes Intra-Exchange Priority resolution (3 > 2 > 1, with ingested_at tie-break)
on business key: (exchange, raw_symbol, timestamp).
Alpaca and Binance records remain strictly independent and are both preserved in Curated Silver.
Uses PyIceberg + DuckDB streaming symbol-level append for zero JVM heap pressure and high throughput.
In accordance with PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md Sections 13, 14, 15.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
import sys
import time
import uuid

import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, EqualTo


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Silver to Curated Silver Priority Merge"
    )
    parser.add_argument(
        "--source-table",
        default="silver.crypto_bars",
        help="Source Iceberg Silver table name (e.g. silver.crypto_bars)",
    )
    parser.add_argument(
        "--target-table",
        default="silver.curated_crypto_bars",
        help="Target Iceberg Curated Silver table name (e.g. silver.curated_crypto_bars)",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional batch filter",
    )
    return parser.parse_args()


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


def discover_symbols(staging_path: str | None = None) -> list[tuple[str, str]]:
    resolved_path = staging_path or "/home/iceberg/warehouse_local/staging/canonical"
    if not os.path.exists(resolved_path):
        resolved_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../../warehouse/staging/canonical")
        )

    symbols = []
    if os.path.exists(resolved_path):
        import pyarrow.parquet as pq

        for exc in sorted(os.listdir(resolved_path)):
            exc_p = os.path.join(resolved_path, exc)
            if os.path.isdir(exc_p):
                for sym_folder in sorted(os.listdir(exc_p)):
                    sym_p = os.path.join(exc_p, sym_folder)
                    if os.path.isdir(sym_p):
                        files = [f for f in os.listdir(sym_p) if f.endswith(".parquet")]
                        if files:
                            first_file = os.path.join(sym_p, files[0])
                            tbl = pq.read_table(first_file, columns=["raw_symbol"])
                            raw_sym = tbl["raw_symbol"][0].as_py()
                            symbols.append((exc, raw_sym))
    return symbols


def main() -> None:
    args = parse_args()
    source_table_name = args.source_table
    if source_table_name.startswith("lakehouse."):
        source_table_name = source_table_name[len("lakehouse."):]

    target_table_name = args.target_table
    if target_table_name.startswith("lakehouse."):
        target_table_name = target_table_name[len("lakehouse."):]

    batch_id = args.batch_id or str(uuid.uuid4())

    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Silver -> Curated Silver reconciliation...")
    print(f"Reading Silver: {source_table_name}")
    print(f"Target Curated Silver: {target_table_name}")

    catalog = get_catalog()
    source_tbl = catalog.load_table(source_table_name)
    target_tbl = catalog.load_table(target_table_name)
    target_schema = target_tbl.schema().as_arrow()

    symbols = discover_symbols()
    total_symbols = len(symbols)
    print(f"Discovered {total_symbols} symbols to reconcile in Curated Silver.")

    if total_symbols == 0:
        print("No symbols discovered. Exiting.")
        return

    duck_con = duckdb.connect()
    total_merged = 0
    t_start = time.time()

    for idx, (exc, sym) in enumerate(symbols, 1):
        t0 = time.time()
        # Scan Silver for incoming records for this symbol
        scan = source_tbl.scan(
            row_filter=And(EqualTo("exchange", exc), EqualTo("raw_symbol", sym))
        )
        incoming_arr = scan.to_arrow()
        incoming_count = len(incoming_arr)

        if incoming_count == 0:
            print(f"[{exc} {idx}/{total_symbols}] No Silver records found for {sym}. Skipping.")
            continue

        # Reconcile priority (3 > 2 > 1) and tie-break by ingested_at DESC within (exchange, raw_symbol, timestamp)
        # Spec Section 13: intra-exchange priority rule
        reconciled_arr = duck_con.execute("""
            SELECT 
                exchange,
                raw_symbol,
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                trade_count,
                source,
                priority,
                base_currency,
                quote_currency,
                canonical_symbol,
                batch_id,
                ingested_at
            FROM incoming_arr
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY exchange, raw_symbol, timestamp 
                ORDER BY priority DESC, ingested_at DESC
            ) = 1
            ORDER BY timestamp
        """).arrow()

        reconciled_count = len(reconciled_arr)

        # Cast to target table schema
        reconciled_arr = reconciled_arr.cast(target_schema)

        # PyIceberg streaming append to Curated Silver
        target_tbl.append(reconciled_arr)
        total_merged += reconciled_count
        elapsed = time.time() - t0

        print(
            f"[{exc} {idx}/{total_symbols}] Reconciled & appended {reconciled_count:,} rows for {sym} "
            f"into Curated Silver in {elapsed:.1f}s (running total: {total_merged:,})"
        )

    total_elapsed = time.time() - t_start
    print(
        f"\n[{datetime.now(timezone.utc).isoformat()}] Curated Silver reconciliation completed successfully: "
        f"{total_merged:,} records in {total_elapsed:.1f}s ({total_merged / max(total_elapsed, 1):,.0f} rows/s)."
    )

    # Record pipeline audit
    if total_merged > 0:
        try:
            audit_table = catalog.load_table("gold.pipeline_audit")
            now_utc = datetime.now(timezone.utc)
            audit_record = pa.Table.from_pydict(
                {
                    "run_id": [batch_id],
                    "dag_id": ["lakehouse_historical_backfill"],
                    "step_name": ["silver_to_curated_silver"],
                    "status": ["SUCCESS"],
                    "records_processed": [total_merged],
                    "details": [
                        f"Reconciled {total_merged:,} records into Curated Silver in {total_elapsed:.1f}s"
                    ],
                    "created_at": [now_utc],
                },
                schema=audit_table.schema().as_arrow(),
            )
            audit_table.append(audit_record)
            print(f"Logged SUCCESS in lakehouse.gold.pipeline_audit for batch {batch_id}.")
        except Exception as e:
            print(f"Warning: Failed to log audit record: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
