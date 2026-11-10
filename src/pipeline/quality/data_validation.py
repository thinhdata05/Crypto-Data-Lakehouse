"""
Data Quality & Integrity Validation for Multi-Exchange Lakehouse Iceberg Tables.
Validates:
1. Zero duplicate (exchange, raw_symbol, timestamp) business keys in authoritative Silver (`silver.bars`).
2. Market sanity: High >= Low, High >= Open, High >= Close, Low <= Open, Low <= Close, positive prices, non-negative volumes.
3. Multi-exchange integrity: Both Alpaca and Binance records exist independently.
4. Metric counts across Bronze (`bronze.crypto_bars`), Silver (`silver.bars`), and Ops Manifest (`ops.ingestion_manifest`).
Uses Partition-Aligned Quarterly Window Scans (`[YYYY-Q1..Q4)`) with column projection so each daily Parquet file is scanned once
while keeping Peak RAM < 500 MB.
Logs audit metrics into `lakehouse.ops.pipeline_audit`.
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
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan


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


def generate_quarter_windows(start_year: int = 2021, end_year: int | None = None) -> list[tuple[str, str]]:
    if end_year is None:
        end_year = datetime.now(timezone.utc).year
    windows = []
    for yr in range(start_year, end_year + 1):
        windows.append((f"{yr}-01-01T00:00:00+00:00", f"{yr}-04-01T00:00:00+00:00"))
        windows.append((f"{yr}-04-01T00:00:00+00:00", f"{yr}-07-01T00:00:00+00:00"))
        windows.append((f"{yr}-07-01T00:00:00+00:00", f"{yr}-10-01T00:00:00+00:00"))
        windows.append((f"{yr}-10-01T00:00:00+00:00", f"{yr + 1}-01-01T00:00:00+00:00"))
    return windows


def run_validation(
    run_id: str | None = None,
    target_table: str = "silver.bars",
) -> bool:
    run_id = run_id or str(uuid.uuid4())
    catalog = get_catalog()
    t_start = time.time()

    all_passed = True
    print("=" * 80)
    print(f"[{datetime.now(timezone.utc).isoformat()}] STARTING DATA QUALITY & AUDIT CHECKS (Target: {target_table})")
    print(f"Run ID: {run_id}")
    print("=" * 80)

    try:
        tables = {}
        counts = {}
        files_counts = {}
        for tname in ["bronze.crypto_bars", "silver.bars", "ops.ingestion_manifest"]:
            try:
                tbl = catalog.load_table(tname)
                tables[tname] = tbl
                s = tbl.current_snapshot()
                counts[tname] = int(s.summary.get("total-records", 0)) if s else 0
                files_counts[tname] = int(s.summary.get("total-data-files", 0)) if s else 0
            except Exception:
                counts[tname] = 0
                files_counts[tname] = 0

        bronze_count = counts["bronze.crypto_bars"]
        silver_count = counts["silver.bars"]
        manifest_count = counts["ops.ingestion_manifest"]

        print(f"[METRICS] Bronze (bronze.crypto_bars):    {bronze_count:>13,} rows ({files_counts['bronze.crypto_bars']:,} data files)")
        print(f"[METRICS] Silver (silver.bars):           {silver_count:>13,} rows ({files_counts['silver.bars']:,} data files)")
        print(f"[METRICS] Ops Manifest (ops.ingestion_manifest): {manifest_count:>7,} entries")

        if target_table not in tables:
            validated_tbl = catalog.load_table(target_table)
        else:
            validated_tbl = tables[target_table]

        duck_con = duckdb.connect()
        duck_con.execute("SET memory_limit='1GB'")
        duck_con.execute("SET threads=4")
        total_duplicates = 0
        total_sanity_violations = 0
        exchanges_seen: set[str] = set()
        symbols_seen: set[tuple[str, str]] = set()
        quote_currencies_seen: set[str] = set()

        selected_cols = (
            "exchange",
            "raw_symbol",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_currency",
        )

        for w_start, w_end in generate_quarter_windows(2021, datetime.now(timezone.utc).year):
            q_filter = And(
                GreaterThanOrEqual("timestamp", w_start),
                LessThan("timestamp", w_end),
            )
            q_arr = validated_tbl.scan(
                row_filter=q_filter,
                selected_fields=selected_cols,
            ).to_arrow()
            if len(q_arr) == 0:
                del q_arr
                continue

            duck_con.register("q_arr", q_arr)

            meta_rows = duck_con.execute(
                "SELECT DISTINCT exchange, raw_symbol, quote_currency FROM q_arr"
            ).fetchall()
            for exc, r_sym, q_curr in meta_rows:
                exchanges_seen.add(exc)
                symbols_seen.add((exc, r_sym))
                quote_currencies_seen.add(f"{exc}:{q_curr}")

            dups = duck_con.execute("""
                SELECT COUNT(*) FROM (
                    SELECT exchange, raw_symbol, timestamp, COUNT(*) AS cnt
                    FROM q_arr
                    GROUP BY exchange, raw_symbol, timestamp
                    HAVING cnt > 1
                )
            """).fetchone()[0]
            total_duplicates += dups

            violations = duck_con.execute("""
                SELECT COUNT(*) FROM q_arr
                WHERE high < low 
                   OR high < open
                   OR high < close
                   OR low > open
                   OR low > close
                   OR open <= 0.0 
                   OR high <= 0.0
                   OR low <= 0.0
                   OR close <= 0.0 
                   OR volume < 0.0
                   OR (trade_count IS NOT NULL AND trade_count < 0)
                   OR exchange IS NULL 
                   OR raw_symbol IS NULL 
                   OR timestamp IS NULL
            """).fetchone()[0]
            total_sanity_violations += violations

            duck_con.unregister("q_arr")
            del q_arr
            gc.collect()
            pa.default_memory_pool().release_unused()

        if total_duplicates == 0:
            print(f"[PASS] Check 1: {target_table} has 0 duplicate (exchange, raw_symbol, timestamp) keys across {len(symbols_seen)} symbols.")
        else:
            print(f"[FAIL] Check 1: Found {total_duplicates} duplicate keys in {target_table}!")
            all_passed = False

        if total_sanity_violations == 0:
            print(f"[PASS] Check 2: All {target_table} records satisfy 7-rule OHLCV market sanity.")
        else:
            print(f"[FAIL] Check 2: Found {total_sanity_violations} market sanity violations in {target_table}!")
            all_passed = False

        print(f"[METRICS] Distinct Symbols in {target_table}:   {len(symbols_seen)}")
        print(f"[METRICS] Distinct Exchanges in {target_table}: {sorted(list(exchanges_seen))}")
        print(f"[METRICS] Exchange:Quote breakdown: {sorted(list(quote_currencies_seen))}")

        if "alpaca" in exchanges_seen and "binance" in exchanges_seen:
            print("[PASS] Check 3: Multi-exchange independence verified (Both Alpaca and Binance exist).")
        else:
            print(f"[FAIL] Check 3: Missing exchanges! Found: {exchanges_seen}")
            all_passed = False

        elapsed = time.time() - t_start
        status = "SUCCESS" if all_passed else "FAILED"
        details = (
            f"Target={target_table}, Bronze={bronze_count}, Silver={silver_count}, "
            f"Symbols={len(symbols_seen)}, Duplicates={total_duplicates}, "
            f"SanityViolations={total_sanity_violations}, Exchanges={sorted(list(exchanges_seen))}, "
            f"Elapsed={elapsed:.1f}s"
        )

        try:
            audit_table = catalog.load_table("ops.pipeline_audit")
            now_utc = datetime.now(timezone.utc)
            audit_record = pa.Table.from_pydict(
                {
                    "run_id": [run_id],
                    "dag_id": ["data_validation"],
                    "step_name": ["quality_check"],
                    "status": [status],
                    "records_processed": [counts.get(target_table, silver_count)],
                    "details": [details],
                    "created_at": [now_utc],
                },
                schema=audit_table.schema().as_arrow(),
            )
            audit_table.append(audit_record)
        except Exception as audit_err:
            print(f"[INFO] Skipped ops.pipeline_audit write: {audit_err}")

        print("=" * 80)
        print(f"DATA QUALITY STATUS: {status} (Validated in {elapsed:.1f}s)")
        print("=" * 80)
        return all_passed

    except Exception as e:
        print(f"Error during validation: {e}", file=sys.stderr)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Data Quality Validation")
    parser.add_argument("--run-id", default=None, help="Run ID for audit log")
    parser.add_argument(
        "--target-table",
        default="silver.bars",
        help="Target table to validate (default: silver.bars)",
    )
    args = parser.parse_args()

    success = run_validation(args.run_id, target_table=args.target_table)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
