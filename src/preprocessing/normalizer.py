"""
Module for normalizing symbols, timestamps, and validating OHLCV market bars
according to PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md.

Business Key: (exchange, raw_symbol, timestamp).
Derived Metadata: (canonical_symbol, base_currency, quote_currency).
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa

# Baseline cutoff date according to Architecture Spec Section 8
HISTORICAL_CUTOFF_UTC = datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

# Priority constants (applied within each exchange)
INITIAL_REALTIME_PRIORITY = 1
HISTORICAL_PRIORITY = 2
UPDATED_REALTIME_PRIORITY = 3

# Known quote currencies sorted by length descending for greedy matching
KNOWN_QUOTE_CURRENCIES = [
    "FDUSD",
    "USDT",
    "USDC",
    "BUSD",
    "TUSD",
    "USDD",
    "USD",
    "BTC",
    "ETH",
    "BNB",
    "EUR",
    "DAI",
]

# Canonical preprocessed column names
CANONICAL_PREPROCESSED_COLUMNS = [
    "exchange",
    "raw_symbol",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "source",
    "base_currency",
    "quote_currency",
    "canonical_symbol",
    "batch_id",
]

# PyArrow schema for preprocessed canonical files
CANONICAL_PREPROCESSED_ARROW_SCHEMA = pa.schema([
    pa.field("exchange", pa.string()),
    pa.field("raw_symbol", pa.string()),
    pa.field("timestamp", pa.timestamp("us", tz="UTC")),
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.float64()),
    pa.field("trade_count", pa.int64()),
    pa.field("source", pa.string()),
    pa.field("base_currency", pa.string()),
    pa.field("quote_currency", pa.string()),
    pa.field("canonical_symbol", pa.string()),
    pa.field("batch_id", pa.string()),
])


def clean_raw_symbol(raw_symbol: str) -> str:
    """Strips partition prefix (e.g. 'symbol=') and whitespace."""
    if not raw_symbol:
        raise ValueError("Raw symbol cannot be empty.")
    s = str(raw_symbol).strip().upper()
    if s.startswith("SYMBOL="):
        s = s.replace("SYMBOL=", "")
    return s


def split_alpaca_symbol(raw_symbol: str) -> Tuple[str, str, str]:
    """
    Parses an Alpaca raw symbol (e.g., 'BAT/USD', 'symbol=BTC_USD')
    into (base_currency, quote_currency, canonical_symbol).
    Canonical symbol format: '{base}-{quote}'.
    """
    s = clean_raw_symbol(raw_symbol)
    delimiter = "/" if "/" in s else ("_" if "_" in s else "-")

    if delimiter in s:
        parts = [p for p in s.split(delimiter) if p]
        if len(parts) >= 2:
            base, quote = parts[0], parts[1]
            return base, quote, f"{base}-{quote}"

    # Fallback if no delimiter
    return s, "USD", f"{s}-USD"


def split_binance_symbol(raw_symbol: str) -> Tuple[str, str, str]:
    """
    Parses a Binance concatenated raw symbol (e.g., 'BATUSDT', 'AAVEUSDC', 'ETHBTC')
    into (base_currency, quote_currency, canonical_symbol).
    Canonical symbol format: '{base}-{quote}'.
    """
    s = clean_raw_symbol(raw_symbol)

    # If already delimited with '_' or '/' or '-'
    for delim in ("_", "/", "-"):
        if delim in s:
            parts = [p for p in s.split(delim) if p]
            if len(parts) >= 2:
                base, quote = parts[0], parts[1]
                return base, quote, f"{base}-{quote}"

    # Match trailing quote currency
    for quote in KNOWN_QUOTE_CURRENCIES:
        if s.endswith(quote) and len(s) > len(quote):
            base = s[: -len(quote)]
            return base, quote, f"{base}-{quote}"

    # Fallback
    return s, "UNKNOWN", f"{s}-UNKNOWN"


def normalize_timestamp(raw_ts: Any) -> datetime:
    """
    Converts various timestamp formats (pandas Timestamp, ISO string, milliseconds,
    nanoseconds, Unix epoch seconds) to timezone-aware UTC datetime.
    """
    if raw_ts is None:
        raise ValueError("Timestamp cannot be null.")

    if isinstance(raw_ts, datetime):
        if raw_ts.tzinfo is None:
            return raw_ts.replace(tzinfo=timezone.utc)
        return raw_ts.astimezone(timezone.utc)

    if isinstance(raw_ts, str):
        cleaned = raw_ts.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(cleaned)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass

    if isinstance(raw_ts, (int, float)):
        if raw_ts > 1e16:  # Nanos
            sec = raw_ts / 1e9
        elif raw_ts > 1e13:  # Micros
            sec = raw_ts / 1e6
        elif raw_ts > 1e10:  # Millis
            sec = raw_ts / 1e3
        else:
            sec = float(raw_ts)

        return datetime.fromtimestamp(sec, tz=timezone.utc)

    if hasattr(raw_ts, "to_pydatetime"):
        dt = raw_ts.to_pydatetime()
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    raise ValueError(f"Unsupported timestamp format: {type(raw_ts)}: {raw_ts}")


def validate_ohlcv_record(
    open_val: float,
    high_val: float,
    low_val: float,
    close_val: float,
    volume_val: float,
    trade_count_val: Optional[int] = 0,
) -> Tuple[bool, Optional[str]]:
    """
    Validates logical market sanity of an OHLCV candle.
    Returns (is_valid, reason).
    """
    if any(v is None for v in (open_val, high_val, low_val, close_val, volume_val)):
        return False, "Null value in core price/volume fields"

    if open_val <= 0 or close_val <= 0 or high_val <= 0 or low_val <= 0:
        return False, "Non-positive price encountered"

    if volume_val < 0:
        return False, "Negative volume encountered"

    if trade_count_val is not None and trade_count_val < 0:
        return False, "Negative trade count encountered"

    if high_val < low_val:
        return False, f"High ({high_val}) cannot be lower than Low ({low_val})"

    if open_val > high_val or close_val > high_val:
        return False, "Open or Close exceeds High"

    if open_val < low_val or close_val < low_val:
        return False, "Open or Close is below Low"

    return True, None


OHLCV_SANITY_SQL_PREDICATE = """
    open > 0.0 AND high > 0.0 AND low > 0.0 AND close > 0.0
    AND volume >= 0.0
    AND high >= low
    AND high >= open
    AND high >= close
    AND low <= open
    AND low <= close
    AND open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL 
    AND close IS NOT NULL AND volume IS NOT NULL
""".strip()

OHLCV_VIOLATION_SQL_PREDICATE = """
    high < low
    OR high < open
    OR high < close
    OR low > open
    OR low > close
    OR open <= 0.0
    OR high <= 0.0
    OR low <= 0.0
    OR close <= 0.0
    OR volume < 0.0
    OR open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR volume IS NULL
""".strip()


BINANCE_PREFERRED_QUOTE_ORDER = ["USDT", "USDC", "USD", "BTC", "ETH", "BNB"]
ALPACA_PREFERRED_QUOTE_ORDER = ["USD", "USDT", "USDC"]


def resolve_preferred_quote_symbols(
    raw_symbols: List[str],
    exchange: str = "binance",
) -> Set[str]:
    """
    Enforces SPEC_FINAL Section 4 Scope Rule:
    Each underlying asset base currency retains exactly 1 quote currency.
    For Binance: prefers USDT > USDC > USD > BTC.
    For Alpaca: prefers USD > USDT > USDC.
    Returns the set of raw symbols selected.
    """
    from collections import defaultdict

    split_fn = split_alpaca_symbol if exchange.lower() == "alpaca" else split_binance_symbol
    order = ALPACA_PREFERRED_QUOTE_ORDER if exchange.lower() == "alpaca" else BINANCE_PREFERRED_QUOTE_ORDER

    # Map base -> list of (priority_rank, raw_sym)
    base_map = defaultdict(list)
    for raw_sym in set(raw_symbols):
        clean_sym = clean_raw_symbol(raw_sym)
        base, quote, _ = split_fn(clean_sym)
        rank = order.index(quote) if quote in order else len(order) + 1
        base_map[base].append((rank, raw_sym))

    selected = set()
    for base, candidates in base_map.items():
        candidates.sort(key=lambda x: x[0])
        selected.add(candidates[0][1])

    return selected

