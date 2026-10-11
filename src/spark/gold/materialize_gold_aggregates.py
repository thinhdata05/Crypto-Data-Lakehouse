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
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Gold Aggregates Materialization...")

    catalog = get_catalog()
    t_gold = catalog.load_table("gold.curated_crypto_bars")
    t_daily = catalog.load_table("gold.agg_daily_crypto_ohlcv")
    target_schema_daily = t_daily.schema().as_arrow()

    symbols = discover_symbols()
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
        t_daily.append(agg_arr)
        total_daily_rows += len(agg_arr)
        elapsed = time.time() - t0
        print(f"[{exc} {idx}/{total_symbols}] Aggregated {len(arr):,} minute bars -> {len(agg_arr):,} daily rows for {sym} in {elapsed:.1f}s")

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
    t_spread.append(spread_arr)
    print(f"Materialized {len(spread_arr):,} daily cross-exchange spread records successfully!")


if __name__ == "__main__":
    main()
