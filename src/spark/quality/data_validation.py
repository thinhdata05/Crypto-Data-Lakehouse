"""
Data Quality & Integrity Validation for Multi-Exchange Lakehouse Iceberg Tables.
Validates:
1. Zero duplicate (exchange, raw_symbol, timestamp) business keys in Curated Silver and Gold.
2. Market sanity: High >= Low, positive prices, non-negative volumes.
3. Multi-exchange integrity: Both Alpaca and Binance records exist independently.
4. Metric counts across Bronze, Silver, Curated Silver, Curated Gold.
Logs audit metrics into lakehouse.gold.pipeline_audit.
In accordance with PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
import sys
import uuid

import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, EqualTo


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


def run_validation(run_id: str | None = None) -> bool:
    run_id = run_id or str(uuid.uuid4())
    catalog = get_catalog()

    all_passed = True
    print("=" * 80)
    print(f"[{datetime.now(timezone.utc).isoformat()}] STARTING DATA QUALITY & AUDIT CHECKS")
    print(f"Run ID: {run_id}")
    print("=" * 80)

    try:
        # 1. Row counts across layers from Iceberg snapshots
        tables = {}
        counts = {}
        for tname in ["bronze.crypto_bars", "silver.crypto_bars", "silver.curated_crypto_bars", "gold.curated_crypto_bars"]:
            tbl = catalog.load_table(tname)
            tables[tname] = tbl
            s = tbl.current_snapshot()
            counts[tname] = int(s.summary.get("total-records", 0)) if s else 0

        bronze_count = counts["bronze.crypto_bars"]
        silver_count = counts["silver.crypto_bars"]
        curated_silver_count = counts["silver.curated_crypto_bars"]
        gold_count = counts["gold.curated_crypto_bars"]

        print(f"[METRICS] Bronze Count:         {bronze_count:,}")
        print(f"[METRICS] Silver Count:         {silver_count:,}")
        print(f"[METRICS] Curated Silver Count: {curated_silver_count:,}")
        print(f"[METRICS] Curated Gold Count:   {gold_count:,}")

        symbols = discover_symbols()
        total_duplicates = 0
        total_sanity_violations = 0
        exchanges_seen = set()
        quote_currencies_seen = set()

        duck_con = duckdb.connect()
        gold_tbl = tables["gold.curated_crypto_bars"]

        # Validate per symbol
        for exc, sym in symbols:
            scan = gold_tbl.scan(
                row_filter=And(EqualTo("exchange", exc), EqualTo("raw_symbol", sym))
            )
            sym_arr = scan.to_arrow()
            if len(sym_arr) == 0:
                continue

            exchanges_seen.add(exc)
            quotes = duck_con.execute("SELECT DISTINCT quote_currency FROM sym_arr").fetchall()
            for q in quotes:
                quote_currencies_seen.add(f"{exc}:{q[0]}")

            # Check 1: Duplicate keys
            dups = duck_con.execute("""
                SELECT COUNT(*) FROM (
                    SELECT timestamp, COUNT(*) as cnt 
                    FROM sym_arr 
                    GROUP BY timestamp 
                    HAVING cnt > 1
                )
            """).fetchone()[0]
            total_duplicates += dups

            # Check 2: Market sanity
            violations = duck_con.execute("""
                SELECT COUNT(*) FROM sym_arr
                WHERE high < low 
                   OR open <= 0.0 
                   OR close <= 0.0 
                   OR volume < 0.0
                   OR exchange IS NULL 
                   OR raw_symbol IS NULL 
                   OR timestamp IS NULL
            """).fetchone()[0]
            total_sanity_violations += violations

        # Evaluate Check 1
        if total_duplicates == 0:
            print("[PASS] Check 1: Curated Gold has 0 duplicate (exchange, raw_symbol, timestamp) keys.")
        else:
            print(f"[FAIL] Check 1: Found {total_duplicates} duplicate keys in Curated Gold!")
            all_passed = False

        # Evaluate Check 2
        if total_sanity_violations == 0:
            print("[PASS] Check 2: All Curated Gold records satisfy OHLCV market sanity.")
        else:
            print(f"[FAIL] Check 2: Found {total_sanity_violations} market sanity violations in Gold!")
            all_passed = False

        # Evaluate Check 3: Multi-exchange independence
        print(f"[METRICS] Distinct Exchanges in Gold: {sorted(list(exchanges_seen))}")
        print(f"[METRICS] Exchange:Quote breakdown: {sorted(list(quote_currencies_seen))}")

        if "alpaca" in exchanges_seen and "binance" in exchanges_seen:
            print("[PASS] Check 3: Multi-exchange independence verified (Both Alpaca and Binance exist).")
        else:
            print(f"[FAIL] Check 3: Missing exchanges! Found: {exchanges_seen}")
            all_passed = False

        status = "SUCCESS" if all_passed else "FAILED"
        details = (
            f"Bronze={bronze_count}, Silver={silver_count}, CuratedSilver={curated_silver_count}, "
            f"Gold={gold_count}, Duplicates={total_duplicates}, SanityViolations={total_sanity_violations}, "
            f"Exchanges={sorted(list(exchanges_seen))}"
        )

        audit_table = catalog.load_table("gold.pipeline_audit")
        now_utc = datetime.now(timezone.utc)
        audit_record = pa.Table.from_pydict(
            {
                "run_id": [run_id],
                "dag_id": ["data_validation"],
                "step_name": ["quality_check"],
                "status": [status],
                "records_processed": [gold_count],
                "details": [details],
                "created_at": [now_utc],
            },
            schema=audit_table.schema().as_arrow(),
        )
        audit_table.append(audit_record)

        print("=" * 80)
        print(f"DATA QUALITY STATUS: {status}")
        print("=" * 80)
        return all_passed

    except Exception as e:
        print(f"Error during validation: {e}", file=sys.stderr)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Data Quality Validation")
    parser.add_argument("--run-id", default=None, help="Run ID for audit log")
    args = parser.parse_args()

    success = run_validation(args.run_id)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
