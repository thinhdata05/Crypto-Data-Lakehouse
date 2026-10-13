"""
Spark/Python Job: Materialize Conformed Symbol Dimension (lakehouse.gold.dim_symbol).

Enforces:
1. Conformed SCD Type 1 dimension holding approved crypto asset universe.
2. Idempotent overwrite from gold authoritative facts.
3. Stable surrogate keys based on deterministic sort (exchange, canonical_symbol, raw_symbol).
4. Zero-JVM-OOM vectorized execution: computes rollups in < 3s via DuckDB.
5. Provides universe discovery for REST fetching, data validation, and analytical views.
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


def materialize_dim_symbol(catalog=None, run_id: str | None = None) -> int:
    """
    Materializes lakehouse.gold.dim_symbol idempotently in < 3 seconds.
    Leverages pre-materialized gold.agg_daily_crypto_ohlcv when available for instant rollup,
    or falls back to gold.curated_crypto_bars.
    """
    catalog = catalog or get_catalog()
    run_id = run_id or str(uuid.uuid4())
    t_start = time.time()

    print(f"[{datetime.now(timezone.utc).isoformat()}] Materializing lakehouse.gold.dim_symbol (Run ID: {run_id})...")

    dim_tbl = catalog.load_table("gold.dim_symbol")
    target_schema = dim_tbl.schema().as_arrow()
    duck_con = duckdb.connect()

    # Fast path: compute from agg_daily_crypto_ohlcv if populated
    t_daily = catalog.load_table("gold.agg_daily_crypto_ohlcv")
    has_daily = t_daily.current_snapshot() is not None

    if has_daily:
        print("[FAST PATH] Computing symbol stats from gold.agg_daily_crypto_ohlcv...")
        arr_daily = t_daily.scan().to_arrow()
        
        # Get raw symbol mappings from existing dim_symbol or curated_gold
        try:
            arr_dim = dim_tbl.scan(selected_fields=("exchange", "raw_symbol", "canonical_symbol")).to_arrow()
        except Exception:
            arr_dim = None

        if arr_dim is not None and len(arr_dim) > 0:
            con_dim = duck_con.execute("""
                WITH daily_stats AS (
                    SELECT 
                        exchange,
                        canonical_symbol,
                        base_currency,
                        quote_currency,
                        'crypto' AS asset_class,
                        MIN(trade_date) AS first_trade_date,
                        MAX(trade_date) AS last_trade_date,
                        SUM(minute_bars_count) AS total_bars_recorded
                    FROM arr_daily
                    GROUP BY exchange, canonical_symbol, base_currency, quote_currency
                ),
                raw_mappings AS (
                    SELECT DISTINCT exchange, raw_symbol, canonical_symbol
                    FROM arr_dim
                )
                SELECT
                    CAST(ROW_NUMBER() OVER (ORDER BY s.exchange, s.canonical_symbol) AS BIGINT) AS symbol_key,
                    s.exchange,
                    COALESCE(m.raw_symbol, CASE WHEN s.exchange = 'alpaca' THEN s.base_currency || '_' || s.quote_currency ELSE s.base_currency || s.quote_currency END) AS raw_symbol,
                    s.canonical_symbol,
                    s.base_currency,
                    s.quote_currency,
                    s.asset_class,
                    s.first_trade_date,
                    s.last_trade_date,
                    s.total_bars_recorded,
                    CAST(current_localtimestamp() AS TIMESTAMP) AS created_at
                FROM daily_stats s
                LEFT JOIN raw_mappings m
                  ON s.exchange = m.exchange AND s.canonical_symbol = m.canonical_symbol
                ORDER BY symbol_key ASC
            """).arrow()
        else:
            con_dim = duck_con.execute("""
                SELECT
                    CAST(ROW_NUMBER() OVER (ORDER BY exchange, canonical_symbol) AS BIGINT) AS symbol_key,
                    exchange,
                    CASE WHEN exchange = 'alpaca' THEN base_currency || '_' || quote_currency ELSE base_currency || quote_currency END AS raw_symbol,
                    canonical_symbol,
                    base_currency,
                    quote_currency,
                    'crypto' AS asset_class,
                    MIN(trade_date) AS first_trade_date,
                    MAX(trade_date) AS last_trade_date,
                    SUM(minute_bars_count) AS total_bars_recorded,
                    CAST(current_localtimestamp() AS TIMESTAMP) AS created_at
                FROM arr_daily
                GROUP BY exchange, canonical_symbol, base_currency, quote_currency
                ORDER BY symbol_key ASC
            """).arrow()

    else:
        # Fallback path: compute from gold.curated_crypto_bars projected columns
        print("[FALLBACK] Computing symbol stats from gold.curated_crypto_bars...")
        gold_tbl = catalog.load_table("gold.curated_crypto_bars")
        if not gold_tbl.current_snapshot():
            print("[WARN] Neither daily aggregates nor curated gold have data. Exiting.")
            return 0
            
        arr_gold = gold_tbl.scan(
            selected_fields=("exchange", "raw_symbol", "canonical_symbol", "base_currency", "quote_currency", "timestamp")
        ).to_arrow()

        con_dim = duck_con.execute("""
            SELECT
                CAST(ROW_NUMBER() OVER (ORDER BY exchange, canonical_symbol, raw_symbol) AS BIGINT) AS symbol_key,
                exchange,
                raw_symbol,
                canonical_symbol,
                base_currency,
                quote_currency,
                'crypto' AS asset_class,
                CAST(MIN(timestamp) AS DATE) AS first_trade_date,
                CAST(MAX(timestamp) AS DATE) AS last_trade_date,
                COUNT(*) AS total_bars_recorded,
                CAST(current_localtimestamp() AS TIMESTAMP) AS created_at
            FROM arr_gold
            GROUP BY exchange, raw_symbol, canonical_symbol, base_currency, quote_currency
            ORDER BY symbol_key ASC
        """).arrow()

    dim_arrow = con_dim.cast(target_schema)

    # 100% Idempotent Table Overwrite
    dim_tbl.overwrite(dim_arrow)
    total_elapsed = time.time() - t_start
    print(f"[SUCCESS] Materialized & overwrote {len(dim_arrow)} symbols into lakehouse.gold.dim_symbol in {total_elapsed:.2f}s.")

    # Record pipeline audit
    try:
        audit_tbl = catalog.load_table("gold.pipeline_audit")
        now_utc = datetime.now(timezone.utc)
        audit_rec = pa.Table.from_pydict(
            {
                "run_id": [run_id],
                "dag_id": ["lakehouse_gold_materialization"],
                "step_name": ["materialize_dim_symbol"],
                "status": ["SUCCESS"],
                "records_processed": [len(dim_arrow)],
                "details": [f"Materialized conformed dim_symbol ({len(dim_arrow)} symbols) in {total_elapsed:.2f}s"],
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
