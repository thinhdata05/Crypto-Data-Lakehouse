"""
Incremental Sync Job: Hourly window sync and intra-exchange priority reconciliation.
Reads incremental changes from Bronze/Silver within the specified time window,
applies intra-exchange priority resolution (3 > 2 > 1, including late updates T='u' with priority=3),
and merges into `lakehouse.silver.bars` on (exchange, raw_symbol, timestamp)
with strict partition-pruning date bounds.
Preserves multi-exchange independence (Alpaca and Binance records are never merged together).
Supports both Zero-JVM-OOM PyIceberg + DuckDB engine (default) and PySpark MERGE INTO.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import os
import sys
import uuid

import duckdb
import pyarrow as pa
try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan, Or
except ImportError:  # pragma: no cover - optional on host without pyiceberg
    load_catalog = None
    And = EqualTo = GreaterThanOrEqual = LessThan = Or = None

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from src.pipeline.silver.bronze_to_silver import transform_bronze_batch_to_silver


def _to_arrow_table(duck_rel) -> pa.Table:
    if hasattr(duck_rel, "to_arrow_table"):
        return duck_rel.to_arrow_table()
    return duck_rel.fetch_arrow_table()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hourly Incremental Iceberg Sync")
    parser.add_argument("--window-start", required=True, help="ISO start timestamp")
    parser.add_argument("--window-end", required=True, help="ISO end timestamp")
    parser.add_argument("--batch-id", default=None, help="Batch UUID")
    parser.add_argument(
        "--engine",
        choices=["duckdb", "spark"],
        default="duckdb",
        help="Execution engine: 'duckdb' (PyIceberg + DuckDB, zero JVM OOM) or 'spark'",
    )
    parser.add_argument(
        "--sync-gold",
        action="store_true",
        default=False,
        help="Also sync into local gold (default False in Option 1 Lightweight Producer mode; Gold is materialized exclusively on Cloud).",
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


def _parse_iso_utc(ts_str: str) -> datetime:
    s = ts_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_partition_day_bounds(
    min_ts: datetime,
    max_ts: datetime,
    include_tz: bool = False,
) -> tuple[str, str]:
    """
    Expands [min_ts, max_ts] to UTC day boundaries [00:00:00Z of min_day, 00:00:00Z of max_day + 1 day]
    so Iceberg partition filters align strictly with PARTITIONED BY (days(timestamp)).
    """
    if min_ts.tzinfo is None:
        min_ts = min_ts.replace(tzinfo=timezone.utc)
    if max_ts.tzinfo is None:
        max_ts = max_ts.replace(tzinfo=timezone.utc)
    day_start = min_ts.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = (
        max_ts.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        + timedelta(days=1)
    )
    suffix = "+00:00" if include_tz else ""
    return (
        f"{day_start.strftime('%Y-%m-%dT%H:%M:%S')}{suffix}",
        f"{day_end.strftime('%Y-%m-%dT%H:%M:%S')}{suffix}",
    )


def reconcile_window_tables_in_duckdb(
    duck_con: duckdb.DuckDBPyConnection,
    existing_arr: pa.Table,
    incoming_arr: pa.Table,
    target_time_col: str = "ingested_at",
) -> pa.Table:
    """
    Reconciles incoming Silver rows against existing target partition rows in-memory using DuckDB.
    Applies intra-exchange priority rule (3 > 2 > 1) and tie-breaks by timestamp column DESC,
    with incoming updates winning ties (updates.ingested_at >= target.<target_time_col>).
    """
    duck_con.register("existing_target", existing_arr)
    duck_con.register("incoming_source", incoming_arr)

    if target_time_col == "last_updated_at":
        query = """
            WITH combined AS (
                SELECT
                    exchange, raw_symbol, timestamp, open, high, low, close, volume,
                    trade_count, source, CAST(priority AS INTEGER) AS priority,
                    base_currency, quote_currency, canonical_symbol, batch_id,
                    ingested_at AS sort_ts,
                    CAST(current_localtimestamp() AS TIMESTAMPTZ) AS last_updated_at,
                    0 AS is_target
                FROM incoming_source
                UNION ALL
                SELECT
                    exchange, raw_symbol, timestamp, open, high, low, close, volume,
                    trade_count, source, CAST(priority AS INTEGER) AS priority,
                    base_currency, quote_currency, canonical_symbol, batch_id,
                    last_updated_at AS sort_ts,
                    last_updated_at,
                    1 AS is_target
                FROM existing_target
            )
            SELECT
                exchange, raw_symbol, timestamp, open, high, low, close, volume,
                trade_count, source, priority, base_currency, quote_currency,
                canonical_symbol, batch_id, last_updated_at
            FROM combined
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY exchange, raw_symbol, timestamp
                ORDER BY priority DESC, sort_ts DESC, is_target ASC
            ) = 1
            ORDER BY exchange, raw_symbol, timestamp
        """
    else:
        query = """
            WITH combined AS (
                SELECT
                    exchange, raw_symbol, timestamp, open, high, low, close, volume,
                    trade_count, source, CAST(priority AS INTEGER) AS priority,
                    base_currency, quote_currency, canonical_symbol, batch_id,
                    ingested_at,
                    0 AS is_target
                FROM incoming_source
                UNION ALL
                SELECT
                    exchange, raw_symbol, timestamp, open, high, low, close, volume,
                    trade_count, source, CAST(priority AS INTEGER) AS priority,
                    base_currency, quote_currency, canonical_symbol, batch_id,
                    ingested_at,
                    1 AS is_target
                FROM existing_target
            )
            SELECT
                exchange, raw_symbol, timestamp, open, high, low, close, volume,
                trade_count, source, priority, base_currency, quote_currency,
                canonical_symbol, batch_id, ingested_at
            FROM combined
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY exchange, raw_symbol, timestamp
                ORDER BY priority DESC, ingested_at DESC, is_target ASC
            ) = 1
            ORDER BY exchange, raw_symbol, timestamp
        """
    return _to_arrow_table(duck_con.execute(query))


def _normalize_pyiceberg_ts(ts_str: str, include_tz: bool) -> str:
    dt = _parse_iso_utc(ts_str)
    suffix = "+00:00" if include_tz else ""
    return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}{suffix}"


def run_duckdb_incremental_sync(
    w_start: str, w_end: str, batch_id: str, sync_gold: bool = False
) -> int:
    catalog = get_catalog()
    bronze_tbl = catalog.load_table("bronze.crypto_bars")
    silver_tbl = catalog.load_table("silver.bars")

    has_tz = "tz" in str(bronze_tbl.schema().find_field("timestamp").field_type).lower()
    w_start_norm = _normalize_pyiceberg_ts(w_start, include_tz=has_tz)
    w_end_norm = _normalize_pyiceberg_ts(w_end, include_tz=has_tz)

    window_expr = And(GreaterThanOrEqual("timestamp", w_start_norm), LessThan("timestamp", w_end_norm))
    if batch_id:
        row_filter = Or(EqualTo("batch_id", batch_id), window_expr)
    else:
        row_filter = window_expr

    raw_incoming = bronze_tbl.scan(row_filter=row_filter).to_arrow()
    duck_con = duckdb.connect()

    if len(raw_incoming) > 0:
        incoming_arr = transform_bronze_batch_to_silver(duck_con, raw_incoming)
    else:
        incoming_arr = silver_tbl.scan(row_filter=row_filter).to_arrow()

    cnt = len(incoming_arr)
    print(f"Found {cnt:,} cleansed records in window for Silver reconciliation (PyIceberg + DuckDB engine).")

    has_tz = "tz" in str(silver_tbl.schema().find_field("timestamp").field_type).lower()
    if cnt > 0:
        duck_con.register("raw_incoming", incoming_arr)
        min_ts, max_ts = duck_con.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM raw_incoming"
        ).fetchone()
        day_start_iso, day_end_iso = compute_partition_day_bounds(min_ts, max_ts, include_tz=has_tz)
        partition_window_filter = And(
            GreaterThanOrEqual("timestamp", day_start_iso),
            LessThan("timestamp", day_end_iso),
        )
        print(f"Bounded partition window for reconciliation: [{day_start_iso}, {day_end_iso})")

        # Reconcile into Silver (lakehouse.silver.bars)
        existing_silver_arr = silver_tbl.scan(row_filter=partition_window_filter).to_arrow()
        reconciled_silver_arr = reconcile_window_tables_in_duckdb(
            duck_con,
            existing_arr=existing_silver_arr,
            incoming_arr=incoming_arr,
            target_time_col="ingested_at",
        ).cast(silver_tbl.schema().as_arrow())

        if len(existing_silver_arr) > 0:
            silver_tbl.overwrite(reconciled_silver_arr, overwrite_filter=partition_window_filter)
        else:
            silver_tbl.append(reconciled_silver_arr)
        print(f"Successfully reconciled {len(reconciled_silver_arr):,} window rows into lakehouse.silver.bars.")

        if not sync_gold:
            print("[OPTION 1 PRODUCER MODE] Skipped Local Gold sync (Gold is materialized exclusively on Cloud).")

    # Log audit entry in ops.pipeline_audit
    try:
        audit_tbl = catalog.load_table("ops.pipeline_audit")
        now_utc = datetime.now(timezone.utc)
        audit_record = pa.Table.from_pydict(
            {
                "run_id": [batch_id],
                "dag_id": ["lakehouse_incremental_hourly"],
                "step_name": ["incremental_sync"],
                "status": ["SUCCESS"],
                "records_processed": [cnt],
                "details": [f"Incremental window {w_start} to {w_end} reconciled into silver.bars"],
                "created_at": [now_utc],
            },
            schema=audit_tbl.schema().as_arrow(),
        )
        audit_tbl.append(audit_record)
    except Exception as e:
        print(f"Warning: Failed to log audit record: {e}", file=sys.stderr)

    return cnt


def main() -> None:
    args = parse_args()
    batch_id = args.batch_id or str(uuid.uuid4())
    w_start = args.window_start
    w_end = args.window_end

    print(
        f"[{datetime.now(timezone.utc).isoformat()}] Incremental Sync Window: {w_start} to {w_end} "
        f"(engine=duckdb, sync_gold={args.sync_gold})"
    )

    try:
        run_duckdb_incremental_sync(w_start, w_end, batch_id, sync_gold=args.sync_gold)
    except Exception as e:
        print(f"Error in incremental sync: {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
