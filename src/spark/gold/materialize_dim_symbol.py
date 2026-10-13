"""
Spark/Python Job: Materialize Conformed Symbol Dimension (lakehouse.gold.dim_symbol).

Enforces:
1. Conformed SCD Type 1 dimension holding approved crypto asset universe.
2. Idempotent overwrite from gold.curated_crypto_bars truth.
3. Stable surrogate keys based on deterministic sort (exchange, canonical_symbol, raw_symbol).
4. Provides universe discovery for REST fetching, data validation, and analytical views.
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


def discover_symbols_from_gold(catalog) -> list[tuple[str, str]]:
    """Discover distinct (exchange, raw_symbol) directly from Gold table truth."""
    try:
        gold_tbl = catalog.load_table("gold.curated_crypto_bars")
        if not gold_tbl.current_snapshot():
            return []
        arr = gold_tbl.scan(selected_fields=("exchange", "raw_symbol")).to_arrow()
        if len(arr) == 0:
            return []
        con = duckdb.connect()
        rows = con.execute("SELECT DISTINCT exchange, raw_symbol FROM arr ORDER BY exchange, raw_symbol").fetchall()
        return [(r[0], r[1]) for r in rows]
    except Exception as e:
        print(f"[WARN] Failed to scan gold table for distinct symbols: {e}")
        # Fallback to local staging canonical if table scan fails
        staging_path = "/home/iceberg/warehouse_local/staging/canonical"
        if not os.path.exists(staging_path):
            staging_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../warehouse/staging/canonical"))
        symbols = []
        if os.path.exists(staging_path):
            import pyarrow.parquet as pq
            for exc in sorted(os.listdir(staging_path)):
                exc_p = os.path.join(staging_path, exc)
                if os.path.isdir(exc_p):
                    for sym_folder in sorted(os.listdir(exc_p)):
                        sym_p = os.path.join(exc_p, sym_folder)
                        if os.path.isdir(sym_p):
                            files = [f for f in os.listdir(sym_p) if f.endswith(".parquet")]
                            if files:
                                tbl = pq.read_table(os.path.join(sym_p, files[0]), columns=["raw_symbol"])
                                symbols.append((exc, tbl["raw_symbol"][0].as_py()))
        return symbols


def materialize_dim_symbol(catalog=None, run_id: str | None = None) -> int:
    """Materializes lakehouse.gold.dim_symbol idempotently."""
    catalog = catalog or get_catalog()
    run_id = run_id or str(uuid.uuid4())
    t_start = time.time()

    print(f"[{datetime.now(timezone.utc).isoformat()}] Materializing lakehouse.gold.dim_symbol (Run ID: {run_id})...")

    gold_tbl = catalog.load_table("gold.curated_crypto_bars")
    dim_tbl = catalog.load_table("gold.dim_symbol")
    target_schema = dim_tbl.schema().as_arrow()

    symbols = discover_symbols_from_gold(catalog)
    total_symbols = len(symbols)
    if total_symbols == 0:
        print("[WARN] No symbols discovered to populate gold.dim_symbol. Exiting.")
        return 0

    duck_con = duckdb.connect()
    symbol_rows = []

    for idx, (exc, sym) in enumerate(symbols, 1):
        t0 = time.time()
        sym_filter = And(EqualTo("exchange", exc), EqualTo("raw_symbol", sym))
        scan = gold_tbl.scan(
            row_filter=sym_filter,
            selected_fields=("timestamp", "canonical_symbol", "base_currency", "quote_currency"),
        )
        arr = scan.to_arrow()
        if len(arr) == 0:
            continue

        res = duck_con.execute("""
            SELECT
                $1 AS exchange,
                $2 AS raw_symbol,
                FIRST(canonical_symbol) AS canonical_symbol,
                FIRST(base_currency) AS base_currency,
                FIRST(quote_currency) AS quote_currency,
                'crypto' AS asset_class,
                CAST(MIN(timestamp) AS DATE) AS first_trade_date,
                CAST(MAX(timestamp) AS DATE) AS last_trade_date,
                COUNT(*) AS total_bars_recorded
            FROM arr
        """, [exc, sym]).fetchone()

        symbol_rows.append(res)
        elapsed = time.time() - t0
        print(f"[{exc} {idx}/{total_symbols}] Processed symbol {sym}: {res[8]:,} bars ({res[6]} to {res[7]}) in {elapsed:.2f}s")

    if not symbol_rows:
        print("[WARN] No symbol rows computed. Exiting.")
        return 0

    # Build final Arrow Table with deterministic surrogate key symbol_key
    now_utc = datetime.now(timezone.utc)
    raw_dim_table = pa.Table.from_arrays(
        [
            pa.array([r[0] for r in symbol_rows], type=pa.string()),
            pa.array([r[1] for r in symbol_rows], type=pa.string()),
            pa.array([r[2] for r in symbol_rows], type=pa.string()),
            pa.array([r[3] for r in symbol_rows], type=pa.string()),
            pa.array([r[4] for r in symbol_rows], type=pa.string()),
            pa.array([r[5] for r in symbol_rows], type=pa.string()),
            pa.array([r[6] for r in symbol_rows], type=pa.date32()),
            pa.array([r[7] for r in symbol_rows], type=pa.date32()),
            pa.array([r[8] for r in symbol_rows], type=pa.int64()),
        ],
        names=[
            "exchange", "raw_symbol", "canonical_symbol",
            "base_currency", "quote_currency", "asset_class",
            "first_trade_date", "last_trade_date", "total_bars_recorded"
        ]
    )

    # Deterministic surrogate key and created_at assignment via DuckDB
    dim_arrow = duck_con.execute("""
        SELECT
            CAST(ROW_NUMBER() OVER (ORDER BY exchange, canonical_symbol, raw_symbol) AS BIGINT) AS symbol_key,
            exchange,
            raw_symbol,
            canonical_symbol,
            base_currency,
            quote_currency,
            asset_class,
            first_trade_date,
            last_trade_date,
            total_bars_recorded,
            CAST(current_localtimestamp() AS TIMESTAMP) AS created_at
        FROM raw_dim_table
        ORDER BY symbol_key ASC
    """).arrow()

    dim_arrow = dim_arrow.cast(target_schema)

    # 100% Idempotent Table Overwrite
    dim_tbl.overwrite(dim_arrow)
    total_elapsed = time.time() - t_start
    print(f"\n[SUCCESS] Materialized & overwrote {len(dim_arrow)} symbols into lakehouse.gold.dim_symbol in {total_elapsed:.1f}s.")

    # Record pipeline audit
    try:
        audit_tbl = catalog.load_table("gold.pipeline_audit")
        audit_rec = pa.Table.from_pydict(
            {
                "run_id": [run_id],
                "dag_id": ["lakehouse_gold_materialization"],
                "step_name": ["materialize_dim_symbol"],
                "status": ["SUCCESS"],
                "records_processed": [len(dim_arrow)],
                "details": [f"Materialized conformed dim_symbol ({len(dim_arrow)} symbols) in {total_elapsed:.1f}s"],
                "created_at": [now_utc],
            },
            schema=audit_tbl.schema().as_arrow(),
        )
        audit_tbl.append(audit_rec)
    except Exception as e:
        print(f"[WARN] Failed to write audit log: {e}")

    return len(dim_arrow)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Materialize Conformed Symbol Dimension")
    parser.add_argument("--run-id", default=None, help="Optional pipeline run ID")
    args = parser.parse_args()

    count = materialize_dim_symbol(run_id=args.run_id)
    sys.exit(0 if count > 0 else 1)
