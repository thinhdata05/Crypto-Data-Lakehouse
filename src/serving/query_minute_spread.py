"""
Production Serving API: Parameterized Minute-Level Cross-Exchange Arbitrage Query.

Enforces Lakehouse Safety & Serving Invariants:
1. Replaces unguarded SQL views with bounded, partition-pruned querying.
2. Mandatory time-window bounds: start_time and end_time required.
3. Maximum window enforcement (default <= 7 days) to protect cluster resources.
4. Pushdown partition filters on both Alpaca and Binance scans prior to join.
5. In-process DuckDB normalization with stablecoin peg rates.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import sys

import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual


MAX_ALLOWED_WINDOW_DAYS = 7


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


def parse_iso_datetime(dt_str: str) -> datetime:
    """Parses ISO timestamp string to UTC timezone-aware datetime."""
    try:
        # Handle trailing Z
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception as e:
        raise ValueError(f"Invalid timestamp format '{dt_str}'. Expected ISO format (e.g. 2026-08-01T00:00:00Z): {e}")


def query_minute_spread(
    base_currency: str,
    start_time: str | datetime,
    end_time: str | datetime,
    catalog=None,
    max_window_days: int = MAX_ALLOWED_WINDOW_DAYS,
) -> pa.Table:
    """
    Executes bounded, partition-pruned minute-level cross-exchange spread calculation.
    """
    if isinstance(start_time, str):
        dt_start = parse_iso_datetime(start_time)
    else:
        dt_start = start_time.astimezone(timezone.utc) if start_time.tzinfo else start_time.replace(tzinfo=timezone.utc)

    if isinstance(end_time, str):
        dt_end = parse_iso_datetime(end_time)
    else:
        dt_end = end_time.astimezone(timezone.utc) if end_time.tzinfo else end_time.replace(tzinfo=timezone.utc)

    if dt_start >= dt_end:
        raise ValueError(f"start_time ({dt_start.isoformat()}) must be strictly before end_time ({dt_end.isoformat()}).")

    window_delta = dt_end - dt_start
    if window_delta > timedelta(days=max_window_days):
        raise ValueError(
            f"Requested time window ({window_delta.total_seconds() / 86400:.1f} days) exceeds maximum allowed limit of {max_window_days} days. "
            f"For macro analysis across longer periods, query 'gold.agg_daily_cross_exchange_spread' or view 'v_cross_exchange_daily_spread'."
        )

    cat = catalog or get_catalog()
    gold_tbl = cat.load_table("gold.curated_crypto_bars")

    base_curr = base_currency.upper().strip()

    # Iceberg pushdown row filters
    iso_start = dt_start.isoformat()
    iso_end = dt_end.isoformat()
    time_filter = And(
        GreaterThanOrEqual("timestamp", iso_start),
        LessThanOrEqual("timestamp", iso_end),
    )

    alpaca_filter = And(
        EqualTo("exchange", "alpaca"),
        And(EqualTo("base_currency", base_curr), time_filter),
    )

    binance_filter = And(
        EqualTo("exchange", "binance"),
        And(EqualTo("base_currency", base_curr), time_filter),
    )

    # Scan Alpaca
    scan_alpaca = gold_tbl.scan(
        row_filter=alpaca_filter,
        selected_fields=("base_currency", "timestamp", "close", "volume"),
    )
    arr_alpaca = scan_alpaca.to_arrow()

    # Scan Binance
    scan_binance = gold_tbl.scan(
        row_filter=binance_filter,
        selected_fields=("base_currency", "quote_currency", "timestamp", "close", "volume"),
    )
    arr_binance = scan_binance.to_arrow()

    # Scan FX rate (USDT-USD) for the same bounded window
    rate_filter = And(
        EqualTo("canonical_symbol", "USDT-USD"),
        time_filter,
    )
    scan_rates = gold_tbl.scan(
        row_filter=rate_filter,
        selected_fields=("timestamp", "close"),
    )
    arr_rates = scan_rates.to_arrow()

    if len(arr_alpaca) == 0 or len(arr_binance) == 0:
        # Return empty structured arrow table
        print(f"[INFO] No overlapping minute bars found for {base_curr} between {dt_start.isoformat()} and {dt_end.isoformat()}.")
        return pa.table({
            "base_currency": pa.array([], type=pa.string()),
            "timestamp": pa.array([], type=pa.timestamp("us", tz="UTC")),
            "alpaca_close": pa.array([], type=pa.float64()),
            "alpaca_volume": pa.array([], type=pa.float64()),
            "binance_close_raw": pa.array([], type=pa.float64()),
            "binance_volume": pa.array([], type=pa.float64()),
            "binance_quote": pa.array([], type=pa.string()),
            "usdt_rate_usd": pa.array([], type=pa.float64()),
            "binance_close_usd": pa.array([], type=pa.float64()),
            "spread_usd": pa.array([], type=pa.float64()),
            "spread_pct": pa.array([], type=pa.float64()),
        })

    duck_con = duckdb.connect()
    result_arr = duck_con.execute("""
        SELECT
            a.base_currency,
            a.timestamp,
            a.close AS alpaca_close,
            a.volume AS alpaca_volume,
            b.close AS binance_close_raw,
            b.volume AS binance_volume,
            b.quote_currency AS binance_quote,
            COALESCE(r.close, 1.0) AS usdt_rate_usd,
            ROUND(b.close * COALESCE(r.close, 1.0), 4) AS binance_close_usd,
            ROUND(a.close - (b.close * COALESCE(r.close, 1.0)), 4) AS spread_usd,
            ROUND(((a.close - (b.close * COALESCE(r.close, 1.0))) / (b.close * COALESCE(r.close, 1.0))) * 100, 4) AS spread_pct
        FROM arr_alpaca a
        JOIN arr_binance b
          ON a.timestamp = b.timestamp
        LEFT JOIN arr_rates r
          ON r.timestamp = b.timestamp
        ORDER BY a.timestamp ASC
    """).arrow()

    return result_arr


def main() -> None:
    parser = argparse.ArgumentParser(description="Query Bounded Minute-Level Cross-Exchange Spread")
    parser.add_argument("--base-currency", required=True, help="Base currency symbol (e.g. BTC, ETH)")
    parser.add_argument("--start-time", required=True, help="Window start ISO timestamp (e.g. 2026-08-01T00:00:00Z)")
    parser.add_argument("--end-time", required=True, help="Window end ISO timestamp (e.g. 2026-08-02T00:00:00Z)")
    parser.add_argument("--max-window-days", type=int, default=MAX_ALLOWED_WINDOW_DAYS, help="Max window allowed in days")
    parser.add_argument("--format", choices=["table", "json", "summary"], default="summary", help="Output format")

    args = parser.parse_args()

    try:
        t0 = datetime.now()
        res_table = query_minute_spread(
            base_currency=args.base_currency,
            start_time=args.start_time,
            end_time=args.end_time,
            max_window_days=args.max_window_days,
        )
        elapsed = (datetime.now() - t0).total_seconds()

        print(f"\n[QUERY SUCCESS] Retrieved {len(res_table):,} aligned minute bars in {elapsed:.2f}s.")

        if len(res_table) > 0:
            con = duckdb.connect()
            if args.format == "summary":
                summary = con.execute("""
                    SELECT
                        base_currency,
                        MIN(timestamp) AS window_start,
                        MAX(timestamp) AS window_end,
                        COUNT(*) AS total_bars,
                        ROUND(AVG(spread_usd), 4) AS avg_spread_usd,
                        ROUND(MIN(spread_usd), 4) AS min_spread_usd,
                        ROUND(MAX(spread_usd), 4) AS max_spread_usd,
                        ROUND(AVG(spread_pct), 4) AS avg_spread_pct
                    FROM res_table
                    GROUP BY base_currency
                """).df()
                print("\nSpread Summary:")
                print(summary.to_string(index=False))
            elif args.format == "json":
                df = res_table.to_pandas()
                print(df.head(50).to_json(orient="records", date_format="iso"))
            else:
                print(res_table.to_pandas().head(20).to_string(index=False))

    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
