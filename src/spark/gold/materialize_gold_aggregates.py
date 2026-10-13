"""
Spark/Python Job: Materialize Gold Aggregate & Dimensional Fact Tables.
Populates:
1. lakehouse.gold.agg_daily_crypto_ohlcv (Materialized Daily OHLCV Fact Table)
2. lakehouse.gold.agg_daily_cross_exchange_spread (Materialized Daily Cross-Exchange Comparison Table)
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


def discover_symbols_from_catalog(catalog) -> list[tuple[str, str]]:
    """
    Discovers distinct (exchange, raw_symbol) directly from catalog table truth.
    Prioritizes conformed gold.dim_symbol, falls back to gold.curated_crypto_bars.
    """
    try:
        dim_tbl = catalog.load_table("gold.dim_symbol")
        if dim_tbl.current_snapshot():
            arr = dim_tbl.scan(selected_fields=("exchange", "raw_symbol")).to_arrow()
            if len(arr) > 0:
                con = duckdb.connect()
                rows = con.execute("SELECT DISTINCT exchange, raw_symbol FROM arr ORDER BY exchange, raw_symbol").fetchall()
                print(f"[CATALOG TRUTH] Discovered {len(rows)} symbols from gold.dim_symbol.")
                return [(r[0], r[1]) for r in rows]
    except Exception as e:
        print(f"[WARN] Could not query gold.dim_symbol: {e}")

    try:
        gold_tbl = catalog.load_table("gold.curated_crypto_bars")
        if gold_tbl.current_snapshot():
            arr = gold_tbl.scan(selected_fields=("exchange", "raw_symbol")).to_arrow()
            if len(arr) > 0:
                con = duckdb.connect()
                rows = con.execute("SELECT DISTINCT exchange, raw_symbol FROM arr ORDER BY exchange, raw_symbol").fetchall()
                print(f"[CATALOG TRUTH] Discovered {len(rows)} symbols from gold.curated_crypto_bars.")
                return [(r[0], r[1]) for r in rows]
    except Exception as e:
        print(f"[WARN] Could not query gold.curated_crypto_bars: {e}")

    # Fallback to local staging canonical
    staging_path = "/home/iceberg/warehouse_local/staging/canonical"
    if not os.path.exists(staging_path):
        staging_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../../warehouse/staging/canonical")
        )

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
                            first_file = os.path.join(sym_p, files[0])
                            tbl = pq.read_table(first_file, columns=["raw_symbol"])
                            raw_sym = tbl["raw_symbol"][0].as_py()
                            symbols.append((exc, raw_sym))
    return symbols


def main() -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Gold Aggregates Materialization...")

    catalog = get_catalog()
    t_gold = catalog.load_table("gold.curated_crypto_bars")
    t_daily = catalog.load_table("gold.agg_daily_crypto_ohlcv")
    target_schema_daily = t_daily.schema().as_arrow()

    symbols = discover_symbols_from_catalog(catalog)
    total_symbols = len(symbols)
    duck_con = duckdb.connect()

    t_start = time.time()
    total_daily_rows = 0

    print(f"Aggregating {total_symbols} symbols into lakehouse.gold.agg_daily_crypto_ohlcv...")
    for idx, (exc, sym) in enumerate(symbols, 1):
        t0 = time.time()
        scan = t_gold.scan(row_filter=And(EqualTo("exchange", exc), EqualTo("raw_symbol", sym)))
        arr = scan.to_arrow()
        if len(arr) == 0:
            continue

        agg_arr = duck_con.execute("""
            SELECT
                exchange,
                canonical_symbol,
                base_currency,
                quote_currency,
                CAST(timestamp AS DATE) AS trade_date,
                FIRST(open ORDER BY timestamp ASC) AS open_price,
                MAX(high) AS high_price,
                MIN(low) AS low_price,
                LAST(close ORDER BY timestamp ASC) AS close_price,
                SUM(volume) AS total_volume,
                SUM(trade_count) AS total_trades,
                COUNT(*) AS minute_bars_count,
                CAST(current_localtimestamp() AS TIMESTAMP) AS last_updated_at
            FROM arr
            GROUP BY exchange, canonical_symbol, base_currency, quote_currency, CAST(timestamp AS DATE)
            ORDER BY trade_date ASC
        """).arrow()

        agg_arr = agg_arr.cast(target_schema_daily)
        can_sym = arr["canonical_symbol"][0].as_py()
        sym_filter = And(EqualTo("exchange", exc), EqualTo("canonical_symbol", can_sym))
        try:
            existing_count = len(t_daily.scan(row_filter=sym_filter).to_arrow())
        except Exception:
            existing_count = 0

        if existing_count > 0:
            t_daily.overwrite(agg_arr, overwrite_filter=sym_filter)
            action_desc = "Overwrote (Idempotent Replay)"
        else:
            t_daily.append(agg_arr)
            action_desc = "Appended (Initial Load)"

        total_daily_rows += len(agg_arr)
        elapsed = time.time() - t0
        print(f"[{exc} {idx}/{total_symbols}] {action_desc} {len(arr):,} minute bars -> {len(agg_arr):,} daily rows for {sym} in {elapsed:.1f}s")

    print(f"\nMaterialized {total_daily_rows:,} daily OHLCV records in {time.time()-t_start:.1f}s.")

    # 2. Materialize Daily Cross-Exchange Comparison
    print("Materializing lakehouse.gold.agg_daily_cross_exchange_spread...")
    t_spread = catalog.load_table("gold.agg_daily_cross_exchange_spread")
    target_schema_spread = t_spread.schema().as_arrow()

    all_daily_scan = t_daily.scan().to_arrow()
    spread_arr = duck_con.execute("""
        SELECT
            a.base_currency,
            a.trade_date,
            a.close_price AS alpaca_close,
            a.total_volume AS alpaca_volume,
            b.close_price AS binance_close,
            b.total_volume AS binance_volume,
            (a.close_price - b.close_price) AS spread_abs,
            ROUND(((a.close_price - b.close_price) / b.close_price) * 100, 4) AS spread_pct,
            CAST(current_localtimestamp() AS TIMESTAMP) AS last_updated_at
        FROM all_daily_scan a
        JOIN all_daily_scan b
          ON a.base_currency = b.base_currency
         AND a.trade_date = b.trade_date
        WHERE a.exchange = 'alpaca' AND b.exchange = 'binance'
        ORDER BY a.base_currency, a.trade_date ASC
    """).arrow()

    spread_arr = spread_arr.cast(target_schema_spread)
    # Idempotent write: overwrite the aggregated cross-exchange table
    t_spread.overwrite(spread_arr)
    print(f"Materialized & overwrote {len(spread_arr):,} daily cross-exchange spread records successfully!")

    # 3. Materialize & Refresh Conformed Dimension gold.dim_symbol
    try:
        from src.spark.gold.materialize_dim_symbol import materialize_dim_symbol
        print("\nRefreshing lakehouse.gold.dim_symbol...")
        dim_count = materialize_dim_symbol(catalog=catalog)
        print(f"Refreshed conformed gold.dim_symbol ({dim_count} symbols).")
    except Exception as dim_err:
        print(f"[WARN] Failed to refresh dim_symbol: {dim_err}")


if __name__ == "__main__":
    main()

