"""
Alpaca Source Preprocessor.
Processes raw Alpaca bars, splits symbols into base/quote, normalizes timestamps to UTC,
enforces baseline cutoff (>= 2021-01-01), applies OHLCV sanity checks, and writes canonical staging Parquet.

According to PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md Section 9:
- Dedicated module for Alpaca.
- Preprocessing does NOT assign priority (handled in Spark).
- Exchange is 'alpaca', source is 'historical'.
- Business key: (exchange, raw_symbol, timestamp).
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import glob
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Add project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.preprocessing.normalizer import (
    CANONICAL_PREPROCESSED_ARROW_SCHEMA,
    CANONICAL_PREPROCESSED_COLUMNS,
    HISTORICAL_CUTOFF_UTC,
    clean_raw_symbol,
    resolve_preferred_quote_symbols,
    split_alpaca_symbol,
)


def get_default_paths() -> tuple[str, str]:
    if os.path.exists("/home/iceberg/raw"):
        raw_root = "/home/iceberg/raw/alpaca/bars"
        staging_root = "/home/iceberg/warehouse_local/staging/canonical"
    else:
        raw_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../raw/alpaca/bars")
        )
        staging_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../warehouse/staging/canonical")
        )
    return raw_root, staging_root


def extract_symbol_from_path(path: str) -> Optional[str]:
    parts = os.path.normpath(path).split(os.sep)
    for part in parts:
        if part.startswith("symbol="):
            return part.split("=")[1]
    return None


def process_alpaca_month(
    month_dir: str,
    raw_sym: str,
    year_str: str,
    month_str: str,
    output_dir: str,
    batch_id: str,
) -> Tuple[int, int, Optional[str]]:
    """
    Processes all daily files in a single month directory for Alpaca.
    Reads day files in batch, concatenates into a single Table, normalizes,
    and writes 1 consolidated monthly Parquet file.
    Returns (num_files_processed, num_rows_written, error_message).
    """
    try:
        if not os.path.exists(month_dir):
            return 0, 0, None

        file_names = [f for f in os.listdir(month_dir) if f.endswith(".parquet")]
        if not file_names:
            return 0, 0, None

        file_paths = [os.path.join(month_dir, f) for f in file_names]
        tables = []
        for fp in file_paths:
            try:
                pf = pq.ParquetFile(fp)
                cols = [
                    c
                    for c in ["timestamp", "open", "high", "low", "close", "volume", "trade_count"]
                    if c in pf.schema_arrow.names
                ]
                tables.append(pf.read(columns=cols))
            except Exception as fe:
                print(f"Warning: skipping corrupt file {fp}: {fe}", file=sys.stderr)

        if not tables:
            return len(file_paths), 0, None

        concat_table = pa.concat_tables(tables)
        df = concat_table.to_pandas()
        if df.empty:
            return len(file_paths), 0, None

        # Standardize timestamp to UTC and microsecond precision
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.floor("us")

        # Baseline cutoff >= 2021-01-01 (SPEC Section 8)
        df = df[df["timestamp"] >= HISTORICAL_CUTOFF_UTC]
        if df.empty:
            return len(file_paths), 0, None

        # Market sanity checks
        valid_mask = (
            (df["open"] > 0)
            & (df["high"] > 0)
            & (df["low"] > 0)
            & (df["close"] > 0)
            & (df["high"] >= df["low"])
            & (df["volume"] >= 0)
            & (df["timestamp"].notnull())
        )
        df = df[valid_mask].copy()
        if df.empty:
            return len(file_paths), 0, None

        # Derive metadata
        clean_sym = clean_raw_symbol(raw_sym)
        base_curr, quote_curr, canonical_sym = split_alpaca_symbol(clean_sym)

        df["exchange"] = "alpaca"
        df["raw_symbol"] = clean_sym
        df["base_currency"] = base_curr
        df["quote_currency"] = quote_curr
        df["canonical_symbol"] = canonical_sym
        df["source"] = "historical"
        df["trade_count"] = (
            df["trade_count"].fillna(0).astype("int64")
            if "trade_count" in df.columns
            else 0
        )
        df["batch_id"] = batch_id

        # Write consolidated monthly Parquet
        target_folder = os.path.join(output_dir, "alpaca", canonical_sym)
        os.makedirs(target_folder, exist_ok=True)
        out_filename = f"{canonical_sym}_{year_str}_{month_str}_{batch_id[:8]}.parquet"
        out_path = os.path.join(target_folder, out_filename)

        out_table = pa.Table.from_pandas(
            df[CANONICAL_PREPROCESSED_COLUMNS],
            schema=CANONICAL_PREPROCESSED_ARROW_SCHEMA,
            preserve_index=False,
        )
        pq.write_table(out_table, out_path, compression="snappy")

        return len(file_paths), len(df), None
    except Exception as e:
        return 0, 0, f"Error processing {raw_sym} {year_str}-{month_str}: {e}"


def run_alpaca_preprocessor(
    raw_dir: Optional[str] = None,
    output_dir: Optional[str] = None,
    symbol_filter: Optional[str] = None,
    year_filter: Optional[str] = None,
    batch_id: Optional[str] = None,
    max_workers: Optional[int] = None,
) -> Dict[str, Any]:
    default_raw, default_out = get_default_paths()
    raw_dir = raw_dir or default_raw
    output_dir = output_dir or default_out
    batch_id = batch_id or str(uuid.uuid4())
    workers = max_workers or min(os.cpu_count() or 4, 8)

    t_start = time.time()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Alpaca Preprocessing starting (Workers: {workers})...")
    print(f"Source Raw Directory: {raw_dir}")
    print(f"Target Staging Directory: {output_dir}")

    os.makedirs(output_dir, exist_ok=True)

    if not os.path.exists(raw_dir):
        raise FileNotFoundError(f"Alpaca raw directory not found: {raw_dir}")

    # Discover symbol directories directly
    raw_symbols = [
        d.replace("symbol=", "")
        for d in os.listdir(raw_dir)
        if os.path.isdir(os.path.join(raw_dir, d))
    ]

    if symbol_filter:
        raw_symbols = [s for s in raw_symbols if symbol_filter.upper() in s.upper()]
    else:
        # Enforce Scope Decision (SPEC_FINAL Section 4): Only 1 quote currency per base currency (prefer USD for Alpaca)
        preferred = resolve_preferred_quote_symbols(raw_symbols, exchange="alpaca")
        raw_symbols = [s for s in raw_symbols if s in preferred]
        print(f"Retained {len(raw_symbols)} preferred quote symbols for Alpaca.")

    # Build list of month processing jobs
    jobs = []
    for s in sorted(raw_symbols):
        sym_dir = os.path.join(raw_dir, f"symbol={s}")
        if not os.path.exists(sym_dir):
            sym_dir = os.path.join(raw_dir, s)
            if not os.path.exists(sym_dir):
                continue

        for y_name in sorted(os.listdir(sym_dir)):
            y_path = os.path.join(sym_dir, y_name)
            if not os.path.isdir(y_path):
                continue

            year_val = y_name.replace("year=", "")
            if not year_val.isdigit():
                continue
            if int(year_val) < 2021:
                continue  # Skip pre-2021 immediately at directory level
            if year_filter and str(year_filter) != str(year_val):
                continue

            for m_name in sorted(os.listdir(y_path)):
                m_path = os.path.join(y_path, m_name)
                if not os.path.isdir(m_path):
                    continue
                month_val = m_name.replace("month=", "")
                jobs.append((m_path, s, year_val, month_val, output_dir, batch_id))

    print(f"Found {len(jobs)} monthly batches to process across {len(raw_symbols)} symbols (cutoff >= 2021).")

    total_files = 0
    total_canonical_rows = 0
    completed_months = 0
    errors = []

    if jobs:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_job = {
                executor.submit(process_alpaca_month, *job): job for job in jobs
            }
            for future in as_completed(future_to_job):
                completed_months += 1
                files_cnt, rows_cnt, err = future.result()
                total_files += files_cnt
                total_canonical_rows += rows_cnt
                if err:
                    errors.append(err)

                if completed_months % 100 == 0 or completed_months == len(jobs):
                    pct = (completed_months / len(jobs)) * 100
                    elapsed = time.time() - t_start
                    print(
                        f"[{datetime.now(timezone.utc).isoformat()}] Progress: {completed_months}/{len(jobs)} months ({pct:.1f}%) | "
                        f"Files: {total_files} | Canonical rows: {total_canonical_rows} | Elapsed: {elapsed:.1f}s"
                    )

    elapsed_total = time.time() - t_start
    summary = {
        "exchange": "alpaca",
        "status": "SUCCESS" if not errors else "PARTIAL_SUCCESS",
        "batch_id": batch_id,
        "months_processed": completed_months,
        "files_processed": total_files,
        "canonical_rows": total_canonical_rows,
        "output_dir": output_dir,
        "duration_seconds": round(elapsed_total, 2),
        "errors_count": len(errors),
    }

    print("=" * 80)
    print("ALPACA PREPROCESSING COMPLETED")
    for k, v in summary.items():
        print(f"{k}: {v}")
    if errors:
        print(f"First 5 errors: {errors[:5]}")
    print("=" * 80)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Alpaca Preprocessor")
    parser.add_argument("--raw-dir", default=None, help="Root directory for Alpaca raw files")
    parser.add_argument("--output-dir", default=None, help="Staging output directory")
    parser.add_argument("--symbol", default=None, help="Filter by symbol")
    parser.add_argument("--year", default=None, help="Filter by year")
    parser.add_argument("--batch-id", default=None, help="Batch UUID")
    parser.add_argument("--workers", type=int, default=None, help="Parallel worker count")

    args = parser.parse_args()
    run_alpaca_preprocessor(
        raw_dir=args.raw_dir,
        output_dir=args.output_dir,
        symbol_filter=args.symbol,
        year_filter=args.year,
        batch_id=args.batch_id,
        max_workers=args.workers,
    )


if __name__ == "__main__":
    main()
