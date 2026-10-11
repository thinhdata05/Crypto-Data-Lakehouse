"""
Incremental REST API Fetcher for Alpaca and Binance.
In accordance with PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md Section 19.B:
- Fetches recent minute bars via REST API.
- Normalizes using canonical preprocessor schema.
- Gathers source='historical', priority=HISTORICAL_PRIORITY (priority=2).
- Prepares staging Parquet for downstream Bronze/Silver/Curated Silver/Gold ingestion.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import sys
import urllib.request
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.preprocessing.normalizer import (
    CANONICAL_PREPROCESSED_ARROW_SCHEMA,
    CANONICAL_PREPROCESSED_COLUMNS,
    clean_raw_symbol,
    split_alpaca_symbol,
    split_binance_symbol,
    validate_ohlcv_record,
)


def _load_dotenv_if_present() -> None:
    env_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.env"))
    if os.path.exists(env_file):
        try:
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip().strip("\"'")
                        if k and k not in os.environ:
                            os.environ[k] = v
        except Exception:
            pass

_load_dotenv_if_present()


def fetch_binance_klines(
    symbol: str,
    interval: str = "1m",
    limit: int = 60,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
) -> list[dict]:
    """Fetches public klines from Binance REST API."""
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    if start_time_ms:
        url += f"&startTime={start_time_ms}"
    if end_time_ms:
        url += f"&endTime={end_time_ms}"

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "CryptoLakehouse/1.0"},
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        data = json.loads(response.read().decode("utf-8"))

    results = []
    for item in data:
        # Binance kline format:
        # 0: Open time, 1: Open, 2: High, 3: Low, 4: Close, 5: Volume,
        # 6: Close time, 7: Quote asset volume, 8: Number of trades
        results.append({
            "timestamp": pd.to_datetime(item[0], unit="ms", utc=True),
            "open": float(item[1]),
            "high": float(item[2]),
            "low": float(item[3]),
            "close": float(item[4]),
            "volume": float(item[5]),
            "trade_count": int(item[8]),
        })
    return results


def fetch_alpaca_bars(
    symbol: str,
    start_iso: str | None = None,
    end_iso: str | None = None,
    limit: int = 60,
) -> list[dict]:
    api_key = os.environ.get("APCA_API_KEY_ID") or os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("APCA_API_SECRET_KEY") or os.environ.get("ALPACA_SECRET_KEY")
    if not api_key or not secret_key:
        print(f"[INFO] Alpaca API keys not set. Skipping Alpaca live REST call for {symbol}.")
        return []

    clean_sym = symbol.replace("-", "/").replace("_", "/")
    url = f"https://data.alpaca.markets/v1beta3/crypto/us/bars?symbols={clean_sym}&timeframe=1Min&limit={limit}"
    if start_iso:
        url += f"&start={start_iso}"
    if end_iso:
        url += f"&end={end_iso}"

    req = urllib.request.Request(
        url,
        headers={
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": secret_key,
            "User-Agent": "CryptoLakehouse/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode("utf-8"))
        bars = data.get("bars", {}).get(clean_sym, [])
        results = []
        for item in bars:
            results.append({
                "timestamp": pd.to_datetime(item["t"], utc=True),
                "open": float(item["o"]),
                "high": float(item["h"]),
                "low": float(item["l"]),
                "close": float(item["c"]),
                "volume": float(item["v"]),
                "trade_count": int(item.get("n", 0)),
            })
        return results
    except Exception as e:
        print(f"[WARN] Failed fetching Alpaca bars: {e}")
        return []


def get_approved_universe() -> dict[str, set[str]]:
    """Loads approved canonical symbols per exchange from dim_symbol to enforce Data Governance."""
    try:
        from pyiceberg.catalog import load_catalog
        rest_uri = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
        s3_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
        s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
        cat = load_catalog(
            "lakehouse",
            **{
                "type": "rest",
                "uri": rest_uri,
                "s3.endpoint": s3_endpoint,
                "s3.access-key-id": s3_access_key,
                "s3.secret-access-key": s3_secret_key,
            },
        )
        dim_table = cat.load_table("gold.dim_symbol")
        arrow_tbl = dim_table.scan().to_arrow()
        exchanges = arrow_tbl["exchange"].to_pylist()
        raw_symbols = arrow_tbl["raw_symbol"].to_pylist()
        universe: dict[str, set[str]] = {"binance": set(), "alpaca": set()}
        for exc, sym in zip(exchanges, raw_symbols):
            if exc in universe:
                universe[exc].add(sym)
        return universe
    except Exception as e:
        print(f"[WARN] Could not load universe from dim_symbol: {e}")
        return {}


def run_incremental_fetch(
    binance_symbols: list[str] | None = None,
    alpaca_symbols: list[str] | None = None,
    limit: int = 60,
    output_dir: str | None = None,
    batch_id: str | None = None,
) -> dict:
    batch_id = batch_id or str(uuid.uuid4())
    if not output_dir:
        output_dir = "/home/iceberg/warehouse_local/staging/canonical" if os.path.exists("/home/iceberg") else "warehouse/staging/canonical"

    binance_symbols = binance_symbols or ["BTCUSDT", "ETHUSDT"]
    alpaca_symbols = alpaca_symbols or ["BTC_USD", "ETH_USD"]

    # Enforce Symbol Universe Governance against Gold Dimension Table
    approved_universe = get_approved_universe()
    if approved_universe.get("binance"):
        filtered_binance = [s for s in binance_symbols if s in approved_universe["binance"]]
        rejected_binance = [s for s in binance_symbols if s not in approved_universe["binance"]]
        if rejected_binance:
            print(f"[GOVERNANCE WARNING] Rejected unapproved Binance symbols (violates preferred quote rule): {rejected_binance}")
        binance_symbols = filtered_binance

    if approved_universe.get("alpaca"):
        allowed_alpaca = approved_universe["alpaca"]
        filtered_alpaca = [
            s for s in alpaca_symbols 
            if s in allowed_alpaca or s.replace("/", "_") in allowed_alpaca or s.replace("_", "/") in allowed_alpaca
        ]
        rejected_alpaca = [s for s in alpaca_symbols if s not in filtered_alpaca]
        if rejected_alpaca:
            print(f"[GOVERNANCE WARNING] Rejected unapproved Alpaca symbols: {rejected_alpaca}")
        alpaca_symbols = filtered_alpaca

    total_records = 0
    generated_files = []

    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting incremental REST fetch (Batch: {batch_id})...")
    print(f"Active approved symbols: Binance={binance_symbols}, Alpaca={alpaca_symbols}")

    # 1. Fetch Binance
    for sym in binance_symbols:
        try:
            print(f"Fetching Binance REST bars for {sym}...")
            raw_bars = fetch_binance_klines(sym, interval="1m", limit=limit)
            if not raw_bars:
                continue

            base, quote, canonical = split_binance_symbol(sym)
            valid_rows = []
            for bar in raw_bars:
                valid, _ = validate_ohlcv_record(
                    bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"], bar["trade_count"]
                )
                if not valid:
                    continue
                valid_rows.append({
                    "exchange": "binance",
                    "raw_symbol": sym,
                    "timestamp": bar["timestamp"],
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar["volume"],
                    "trade_count": bar["trade_count"],
                    "source": "historical",  # SPEC Section 19.B: source='historical', priority=2
                    "base_currency": base,
                    "quote_currency": quote,
                    "canonical_symbol": canonical,
                    "batch_id": batch_id,
                })

            if valid_rows:
                df = pd.DataFrame(valid_rows)
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.floor("us")
                target_folder = os.path.join(output_dir, "binance", canonical)
                os.makedirs(target_folder, exist_ok=True)
                out_path = os.path.join(target_folder, f"incremental_{batch_id[:8]}.parquet")
                table = pa.Table.from_pandas(df, schema=CANONICAL_PREPROCESSED_ARROW_SCHEMA, preserve_index=False)
                pq.write_table(table, out_path, compression="snappy")
                total_records += len(df)
                generated_files.append(out_path)
                print(f"Wrote {len(df)} incremental Binance records for {canonical} -> {out_path}")

        except Exception as e:
            print(f"Error fetching Binance {sym}: {e}", file=sys.stderr)

    # 2. Fetch Alpaca
    for sym in alpaca_symbols:
        try:
            print(f"Fetching Alpaca REST bars for {sym}...")
            raw_bars = fetch_alpaca_bars(sym, limit=limit)
            if not raw_bars:
                continue

            base, quote, canonical = split_alpaca_symbol(sym)
            valid_rows = []
            for bar in raw_bars:
                valid, _ = validate_ohlcv_record(
                    bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"], bar["trade_count"]
                )
                if not valid:
                    continue
                valid_rows.append({
                    "exchange": "alpaca",
                    "raw_symbol": sym,
                    "timestamp": bar["timestamp"],
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar["volume"],
                    "trade_count": bar["trade_count"],
                    "source": "historical",
                    "base_currency": base,
                    "quote_currency": quote,
                    "canonical_symbol": canonical,
                    "batch_id": batch_id,
                })

            if valid_rows:
                df = pd.DataFrame(valid_rows)
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.floor("us")
                target_folder = os.path.join(output_dir, "alpaca", canonical)
                os.makedirs(target_folder, exist_ok=True)
                out_path = os.path.join(target_folder, f"incremental_{batch_id[:8]}.parquet")
                table = pa.Table.from_pandas(df, schema=CANONICAL_PREPROCESSED_ARROW_SCHEMA, preserve_index=False)
                pq.write_table(table, out_path, compression="snappy")
                total_records += len(df)
                generated_files.append(out_path)
                print(f"Wrote {len(df)} incremental Alpaca records for {canonical} -> {out_path}")

        except Exception as e:
            print(f"Error fetching Alpaca {sym}: {e}", file=sys.stderr)

    return {
        "status": "SUCCESS",
        "batch_id": batch_id,
        "total_records": total_records,
        "files": generated_files,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Incremental REST API Fetcher")
    parser.add_argument("--binance-symbols", default="AAVEUSDC", help="Comma separated Binance symbols")
    parser.add_argument("--alpaca-symbols", default="AAVE/USD", help="Comma separated Alpaca symbols")
    parser.add_argument("--limit", type=int, default=10, help="Number of bars to fetch")
    parser.add_argument("--output-dir", default=None, help="Output staging directory")
    parser.add_argument("--batch-id", default=None, help="Batch UUID")
    args = parser.parse_args()

    binance_syms = [s.strip() for s in args.binance_symbols.split(",") if s.strip()]
    alpaca_syms = [s.strip() for s in args.alpaca_symbols.split(",") if s.strip()]

    result = run_incremental_fetch(
        binance_symbols=binance_syms,
        alpaca_symbols=alpaca_syms,
        limit=args.limit,
        output_dir=args.output_dir,
        batch_id=args.batch_id,
    )
    print(f"Fetch completed: {result['total_records']} records staged.")


if __name__ == "__main__":
    main()
