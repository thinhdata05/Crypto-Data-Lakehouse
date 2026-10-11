"""
Spark/Python Job: Bronze Iceberg Table -> Silver Iceberg Table (Per-Exchange).
Cleanses, types, validates OHLCV sanity, attaches priority=2 (historical),
and deduplicates within each (exchange, raw_symbol, timestamp) before writing to Silver.
Uses PyIceberg + DuckDB streaming symbol-level append for zero JVM heap pressure and high throughput.
In accordance with PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md.
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

HISTORICAL_PRIORITY = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze to Silver Iceberg Transformation")
    parser.add_argument(
        "--source-table",
        default="bronze.crypto_bars",
        help="Source Iceberg Bronze table name (e.g. bronze.crypto_bars)",
    )
    parser.add_argument(
        "--target-table",
        default="silver.crypto_bars",
        help="Target Iceberg Silver table name (e.g. silver.crypto_bars)",
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


def discover_symbols(staging_path: str | None = None, batch_id: str | None = None) -> list[tuple[str, str]]:
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
                        files = [
                            f for f in os.listdir(sym_p)
                            if f.endswith(".parquet") and (not batch_id or batch_id[:8] in f)
                        ]
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

    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Bronze -> Silver transformation...")
    print(f"Reading Bronze: {source_table_name}")
    print(f"Writing Silver: {target_table_name}")

    catalog = get_catalog()
    source_tbl = catalog.load_table(source_table_name)
    target_tbl = catalog.load_table(target_table_name)
    target_schema = target_tbl.schema().as_arrow()

    symbols = discover_symbols(batch_id=args.batch_id)
    total_symbols = len(symbols)
    print(f"Discovered {total_symbols} symbols to process in Bronze -> Silver.")

    if total_symbols == 0:
        print("No symbols discovered. Exiting.")
        return

    duck_con = duckdb.connect()
    total_cleansed = 0
    t_start = time.time()

    for idx, (exc, sym) in enumerate(symbols, 1):
        t0 = time.time()
        # Scan Bronze for this specific symbol
        row_filter = And(EqualTo("exchange", exc), EqualTo("raw_symbol", sym))
        if args.batch_id:
            row_filter = And(row_filter, EqualTo("batch_id", args.batch_id))

        scan = source_tbl.scan(row_filter=row_filter)
        arr = scan.to_arrow()
        raw_count = len(arr)

        if raw_count == 0:
            print(f"[{exc} {idx}/{total_symbols}] No Bronze records found for {sym}. Skipping.")
            continue

        # DuckDB: 1) Market sanity validation, 2) priority=2, 3) Deduplication within batch
        clean_arr = duck_con.execute("""
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
                CAST(2 AS INTEGER) AS priority,
                base_currency,
                quote_currency,
                canonical_symbol,
                batch_id,
                ingested_at
            FROM arr
            WHERE open > 0 AND high > 0 AND low > 0 AND close > 0 
              AND high >= low AND volume >= 0
              AND open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL 
              AND close IS NOT NULL AND volume IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY exchange, raw_symbol, timestamp 
                ORDER BY ingested_at DESC
            ) = 1
            ORDER BY timestamp
        """).arrow()

        clean_count = len(clean_arr)
        dropped_count = raw_count - clean_count

        # Cast to target table schema
        clean_arr = clean_arr.cast(target_schema)

        # PyIceberg streaming append to Silver
        target_tbl.append(clean_arr)
        total_cleansed += clean_count
        elapsed = time.time() - t0

        drop_msg = f" (filtered {dropped_count:,} invalid/dup)" if dropped_count > 0 else ""
        print(
            f"[{exc} {idx}/{total_symbols}] Cleansed & appended {clean_count:,} rows for {sym} "
            f"in {elapsed:.1f}s{drop_msg} (running total: {total_cleansed:,})"
        )

    total_elapsed = time.time() - t_start
    print(
        f"\n[{datetime.now(timezone.utc).isoformat()}] Successfully cleansed and wrote {total_cleansed:,} "
        f"Silver records to {target_table_name} in {total_elapsed:.1f}s ({total_cleansed / max(total_elapsed, 1):,.0f} rows/s)."
    )

    # Record pipeline audit
    if total_cleansed > 0:
        try:
            audit_table = catalog.load_table("gold.pipeline_audit")
            now_utc = datetime.now(timezone.utc)
            audit_record = pa.Table.from_pydict(
                {
                    "run_id": [batch_id],
                    "dag_id": ["lakehouse_historical_backfill"],
                    "step_name": ["bronze_to_silver"],
                    "status": ["SUCCESS"],
                    "records_processed": [total_cleansed],
                    "details": [
                        f"Cleansed {total_cleansed:,} records into Silver in {total_elapsed:.1f}s"
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
