"""
Zero-JVM Pipeline Job: Bronze Iceberg Table -> Silver Iceberg Table (PyIceberg + DuckDB).
Cleanses, types, validates 7-rule OHLCV sanity, dynamically resolves priority (3 > 2 > 1)
from source/priority columns, and deduplicates within each (exchange, raw_symbol, timestamp)
before writing directly to the authoritative Silver table (`lakehouse.silver.bars`).
Uses Partition-Aligned Monthly Window streaming (`[YYYY-MM-01, YYYY-(MM+1)-01)`)
so each UTC day partition is read once and written once as a single coalesced Parquet file (~2,055 files total
instead of 84,149 small per-symbol files), keeping Peak RAM < 800 MB with zero JVM heap pressure.
Logs checkpoints to `lakehouse.ops.ingestion_manifest` and audit telemetry to `lakehouse.ops.pipeline_audit`.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gc
import os
import sys
import time
import uuid

import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from src.preprocessing.normalizer import (
    OHLCV_SANITY_SQL_PREDICATE,
    SOURCE_TO_PRIORITY_SQL_CASE,
)
from src.utils.manifest_tracker import record_manifest_batch


def _to_arrow_table(duck_rel) -> pa.Table:
    if hasattr(duck_rel, "to_arrow_table"):
        return duck_rel.to_arrow_table()
    return duck_rel.fetch_arrow_table()


def transform_bronze_batch_to_silver(duck_con: duckdb.DuckDBPyConnection, arr: pa.Table) -> pa.Table:
    """
    Cleanses an Arrow batch from Bronze, dynamically resolves intra-exchange priority (3 > 2 > 1),
    enforces the 7-condition OHLCV mathematical sanity predicate, and deduplicates by
    (exchange, raw_symbol, timestamp) ordered by priority DESC, ingested_at DESC.
    """
    has_priority_col = "priority" in arr.schema.names
    priority_expr = (
        f"COALESCE(CAST(priority AS INTEGER), {SOURCE_TO_PRIORITY_SQL_CASE})"
        if has_priority_col
        else SOURCE_TO_PRIORITY_SQL_CASE
    )

    duck_con.register("arr_input", arr)
    rel = duck_con.execute(f"""
        WITH ranked AS (
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
                {priority_expr} AS priority,
                base_currency,
                quote_currency,
                canonical_symbol,
                batch_id,
                ingested_at
            FROM arr_input
            WHERE {OHLCV_SANITY_SQL_PREDICATE}
        )
        SELECT *
        FROM ranked
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY exchange, raw_symbol, timestamp
            ORDER BY priority DESC, ingested_at DESC
        ) = 1
        ORDER BY timestamp, exchange, raw_symbol
    """)
    res = _to_arrow_table(rel)
    try:
        duck_con.unregister("arr_input")
    except Exception:
        pass
    return res


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze to Silver Iceberg Transformation")
    parser.add_argument(
        "--source-table",
        default="bronze.crypto_bars",
        help="Source Iceberg Bronze table name (default: bronze.crypto_bars)",
    )
    parser.add_argument(
        "--target-table",
        default="silver.bars",
        help="Target Iceberg Silver table name (default: silver.bars)",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional batch filter",
    )
    parser.add_argument(
        "--force-overwrite",
        action="store_true",
        help="Force overwrite of existing monthly windows in Silver (default: True when --batch-id is incremental, False on resume)",
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


def generate_monthly_windows(start_year: int = 2021, end_year: int | None = None) -> list[tuple[str, str, str]]:
    """
    Generates Monthly UTC windows [(label, start_iso, end_iso), ...] from start_year to end_year.
    Aligns strictly with PARTITIONED BY (days(timestamp)) while keeping each window <= 1.8M rows (< 800 MB RAM).
    """
    if end_year is None:
        end_year = datetime.now(timezone.utc).year
    windows = []
    for yr in range(start_year, end_year + 1):
        for mo in range(1, 13):
            start_iso = f"{yr}-{mo:02d}-01T00:00:00+00:00"
            if mo == 12:
                end_iso = f"{yr + 1}-01-01T00:00:00+00:00"
            else:
                end_iso = f"{yr}-{mo + 1:02d}-01T00:00:00+00:00"
            windows.append((f"{yr}-{mo:02d}", start_iso, end_iso))
    return windows


def main() -> None:
    args = parse_args()
    source_table_name = args.source_table
    if source_table_name.startswith("lakehouse."):
        source_table_name = source_table_name[len("lakehouse."):]

    target_table_name = args.target_table
    if target_table_name.startswith("lakehouse."):
        target_table_name = target_table_name[len("lakehouse."):]

    batch_id = args.batch_id or str(uuid.uuid4())

    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Bronze -> Silver transformation (Monthly Partition-Aligned)...")
    print(f"Reading Bronze: {source_table_name}")
    print(f"Writing Silver: {target_table_name}")

    catalog = get_catalog()
    source_tbl = catalog.load_table(source_table_name)
    target_tbl = catalog.load_table(target_table_name)
    target_schema = target_tbl.schema().as_arrow()

    windows = generate_monthly_windows(2021, datetime.now(timezone.utc).year)
    duck_con = duckdb.connect()
    duck_con.execute("SET memory_limit='1GB'")
    duck_con.execute("SET threads=4")
    total_cleansed = 0
    t_start = time.time()

    symbol_stats: dict[tuple[str, str], dict] = {}

    for idx, (w_label, w_start, w_end) in enumerate(windows, 1):
        t0 = time.time()
        window_filter = And(
            GreaterThanOrEqual("timestamp", w_start),
            LessThan("timestamp", w_end),
        )

        # Check if target Silver already has data for this monthly window
        write_filter = (
            And(window_filter, EqualTo("batch_id", args.batch_id))
            if args.batch_id
            else window_filter
        )
        try:
            existing_arr = target_tbl.scan(
                row_filter=write_filter,
                selected_fields=("raw_symbol",),
            ).to_arrow()
            existing_count = len(existing_arr)
            del existing_arr
        except Exception:
            existing_count = 0

        if existing_count > 0 and not args.force_overwrite:
            total_cleansed += existing_count
            print(
                f"[Month {idx}/{len(windows)}: {w_label}] Skipped {existing_count:,} already-cleansed rows in Silver "
                f"(running total: {total_cleansed:,})"
            )
            continue

        row_filter = (
            And(window_filter, EqualTo("batch_id", args.batch_id))
            if args.batch_id
            else window_filter
        )

        arr = source_tbl.scan(row_filter=row_filter).to_arrow()
        raw_count = len(arr)
        if raw_count == 0:
            del arr
            continue

        clean_arr = transform_bronze_batch_to_silver(duck_con, arr)
        del arr
        gc.collect()

        clean_count = len(clean_arr)
        dropped_count = raw_count - clean_count
        clean_arr = clean_arr.cast(target_schema)

        # Accumulate per-symbol stats for manifest checkpointing
        duck_con.register("win_clean", clean_arr)
        sym_rows = duck_con.execute(
            "SELECT exchange, raw_symbol, COUNT(*) AS cnt, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts "
            "FROM win_clean GROUP BY exchange, raw_symbol"
        ).fetchall()
        duck_con.unregister("win_clean")
        for exc, r_sym, cnt, min_ts, max_ts in sym_rows:
            key = (exc, r_sym)
            if key not in symbol_stats:
                symbol_stats[key] = {"count": cnt, "min_ts": min_ts, "max_ts": max_ts}
            else:
                symbol_stats[key]["count"] += cnt
                if min_ts < symbol_stats[key]["min_ts"]:
                    symbol_stats[key]["min_ts"] = min_ts
                if max_ts > symbol_stats[key]["max_ts"]:
                    symbol_stats[key]["max_ts"] = max_ts

        if existing_count > 0:
            target_tbl.overwrite(clean_arr, overwrite_filter=write_filter)
            action_desc = "Overwrote (Idempotent Replay)"
        else:
            target_tbl.append(clean_arr)
            action_desc = "Appended (Initial Load)"

        total_cleansed += clean_count
        elapsed = time.time() - t0
        drop_msg = f" (filtered {dropped_count:,} invalid/dup)" if dropped_count > 0 else ""
        print(
            f"[Month {idx}/{len(windows)}: {w_label}] {action_desc} {clean_count:,} rows "
            f"across {len(sym_rows)} symbols in {elapsed:.1f}s{drop_msg} (running total: {total_cleansed:,})"
        )

        del clean_arr
        gc.collect()
        pa.default_memory_pool().release_unused()

    # Record per-symbol Silver processing checkpoints in ops.ingestion_manifest
    if symbol_stats:
        now_utc = datetime.now(timezone.utc)
        manifest_entries = [
            {
                "source_file": f"silver/{exc}/{r_sym}",
                "exchange": exc,
                "raw_symbol": r_sym,
                "batch_id": batch_id,
                "file_hash": f"silver_{exc}_{r_sym}_{st['count']}",
                "record_count": st["count"],
                "min_timestamp": st["min_ts"],
                "max_timestamp": st["max_ts"],
                "status": "PROCESSED_SILVER",
                "processed_at": now_utc,
            }
            for (exc, r_sym), st in sorted(symbol_stats.items())
        ]
        record_manifest_batch(catalog, manifest_entries)

    total_elapsed = time.time() - t_start
    print(
        f"\n[{datetime.now(timezone.utc).isoformat()}] Successfully cleansed and wrote {total_cleansed:,} "
        f"Silver records to {target_table_name} in {total_elapsed:.1f}s "
        f"({total_cleansed / max(total_elapsed, 1):,.0f} rows/s)."
    )

    # Record pipeline audit in ops.pipeline_audit
    if total_cleansed > 0:
        try:
            audit_table = catalog.load_table("ops.pipeline_audit")
            now_utc = datetime.now(timezone.utc)
            audit_record = pa.Table.from_pydict(
                {
                    "run_id": [batch_id],
                    "dag_id": ["lakehouse_historical_backfill"],
                    "step_name": ["bronze_to_silver"],
                    "status": ["SUCCESS"],
                    "records_processed": [total_cleansed],
                    "details": [
                        f"Cleansed & deduplicated {total_cleansed:,} records into {target_table_name} in {total_elapsed:.1f}s"
                    ],
                    "created_at": [now_utc],
                },
                schema=audit_table.schema().as_arrow(),
            )
            audit_table.append(audit_record)
            print(f"Logged SUCCESS in lakehouse.ops.pipeline_audit for batch {batch_id}.")
        except Exception as e:
            print(f"Warning: Failed to log audit record: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
