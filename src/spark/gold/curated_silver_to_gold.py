"""
Spark/Python Job: Curated Silver Table -> Curated Gold Table.
Populates authoritative dimensional dataset lakehouse.gold.curated_crypto_bars
from lakehouse.silver.curated_crypto_bars, keeping Alpaca and Binance as separate exchanges.
Uses PyIceberg + DuckDB streaming symbol-level append for zero JVM heap pressure and high throughput.
In accordance with PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md Sections 16, 17.
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
    parser = argparse.ArgumentParser(description="Curated Silver to Gold Ingestion")
    parser.add_argument(
        "--source-table",
        default="silver.curated_crypto_bars",
        help="Source Iceberg Curated Silver table name",
    )
    parser.add_argument(
        "--target-table",
        default="gold.curated_crypto_bars",
        help="Target Iceberg Curated Gold table name",
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

    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Curated Silver -> Curated Gold sync...")
    print(f"Reading Curated Silver: {source_table_name}")
    print(f"Writing Curated Gold: {target_table_name}")

    catalog = get_catalog()
    source_tbl = catalog.load_table(source_table_name)
    target_tbl = catalog.load_table(target_table_name)
    target_schema = target_tbl.schema().as_arrow()

    symbols = discover_symbols()
    total_symbols = len(symbols)
    print(f"Discovered {total_symbols} symbols to sync into Curated Gold.")

    if total_symbols == 0:
        print("No symbols discovered. Exiting.")
        return

    duck_con = duckdb.connect()
    total_synced = 0
    t_start = time.time()

    for idx, (exc, sym) in enumerate(symbols, 1):
        t0 = time.time()
        # Scan Curated Silver for incoming records for this symbol
        scan = source_tbl.scan(
            row_filter=And(EqualTo("exchange", exc), EqualTo("raw_symbol", sym))
        )
        curated_silver_arr = scan.to_arrow()
        rows_count = len(curated_silver_arr)

        if rows_count == 0:
            print(f"[{exc} {idx}/{total_symbols}] No Curated Silver records found for {sym}. Skipping.")
            continue

        # Map to Gold schema: ingested_at -> last_updated_at (now UTC)
        gold_arr = duck_con.execute("""
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
                current_localtimestamp() AS last_updated_at
            FROM curated_silver_arr
            ORDER BY timestamp
        """).arrow()

        # Cast to target table schema
        gold_arr = gold_arr.cast(target_schema)

        # PyIceberg streaming append to Curated Gold
        target_tbl.append(gold_arr)
        total_synced += rows_count
        elapsed = time.time() - t0

        print(
            f"[{exc} {idx}/{total_symbols}] Synced {rows_count:,} rows for {sym} "
            f"into Curated Gold in {elapsed:.1f}s (running total: {total_synced:,})"
        )

    total_elapsed = time.time() - t_start
    print(
        f"\n[{datetime.now(timezone.utc).isoformat()}] Curated Gold sync completed successfully: "
        f"{total_synced:,} records in {total_elapsed:.1f}s ({total_synced / max(total_elapsed, 1):,.0f} rows/s)."
    )

    # Record pipeline audit
    if total_synced > 0:
        try:
            audit_table = catalog.load_table("gold.pipeline_audit")
            now_utc = datetime.now(timezone.utc)
            audit_record = pa.Table.from_pydict(
                {
                    "run_id": [batch_id],
                    "dag_id": ["lakehouse_historical_backfill"],
                    "step_name": ["curated_silver_to_gold"],
                    "status": ["SUCCESS"],
                    "records_processed": [total_synced],
                    "details": [
                        f"Synced {total_synced:,} records into Curated Gold in {total_elapsed:.1f}s"
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
