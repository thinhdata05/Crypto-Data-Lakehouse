"""
Unit tests for symbol splitting, timestamp parsing, and OHLCV sanity checks
in accordance with PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md.
"""

from datetime import datetime, timezone
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.preprocessing.normalizer import (
    HISTORICAL_CUTOFF_UTC,
    normalize_timestamp,
    split_alpaca_symbol,
    split_binance_symbol,
    validate_ohlcv_record,
)


class TestNormalizer(unittest.TestCase):

    def test_split_alpaca_symbol(self):
        base, quote, canonical = split_alpaca_symbol("BAT/USD")
        self.assertEqual(base, "BAT")
        self.assertEqual(quote, "USD")
        self.assertEqual(canonical, "BAT-USD")

        base, quote, canonical = split_alpaca_symbol("symbol=BTC_USD")
        self.assertEqual(base, "BTC")
        self.assertEqual(quote, "USD")
        self.assertEqual(canonical, "BTC-USD")

    def test_split_binance_symbol(self):
        base, quote, canonical = split_binance_symbol("BATUSDT")
        self.assertEqual(base, "BAT")
        self.assertEqual(quote, "USDT")
        self.assertEqual(canonical, "BAT-USDT")

        base, quote, canonical = split_binance_symbol("ETHUSDC")
        self.assertEqual(base, "ETH")
        self.assertEqual(quote, "USDC")
        self.assertEqual(canonical, "ETH-USDC")

        base, quote, canonical = split_binance_symbol("ETHBTC")
        self.assertEqual(base, "ETH")
        self.assertEqual(quote, "BTC")
        self.assertEqual(canonical, "ETH-BTC")

    def test_timestamp_normalization(self):
        dt1 = normalize_timestamp("2024-01-15T12:30:00Z")
        self.assertEqual(dt1, datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc))

        ms_ts = 1705321800000  # Milliseconds
        dt2 = normalize_timestamp(ms_ts)
        self.assertEqual(dt2, datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc))

        ns_ts = 1705321800000000000  # Nanoseconds
        dt3 = normalize_timestamp(ns_ts)
        self.assertEqual(dt3, datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc))

    def test_historical_cutoff(self):
        pre_2021 = datetime(2020, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        post_2021 = datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        self.assertLess(pre_2021, HISTORICAL_CUTOFF_UTC)
        self.assertGreaterEqual(post_2021, HISTORICAL_CUTOFF_UTC)

    def test_validate_ohlcv_sanity(self):
        valid, err = validate_ohlcv_record(100.0, 105.0, 95.0, 102.0, 10.5, 50)
        self.assertTrue(valid)
        self.assertIsNone(err)

        # Invalid: High < Low
        valid, err = validate_ohlcv_record(100.0, 90.0, 95.0, 92.0, 10.5)
        self.assertFalse(valid)

        # Invalid: Non-positive price
        valid, err = validate_ohlcv_record(-100.0, 105.0, 95.0, 102.0, 10.5)
        self.assertFalse(valid)

        # Invalid: Negative volume
        valid, err = validate_ohlcv_record(100.0, 105.0, 95.0, 102.0, -1.0)
        self.assertFalse(valid)

    def test_resolve_preferred_quote_symbols_binance(self):
        """
        Verify SPEC_FINAL Section 4 Scope Rule:
        If both SOLUSDT and SOLUSDC exist, USDT is preferred and USDC is dropped.
        If only AAVEUSDC exists, AAVEUSDC is retained.
        """
        from src.preprocessing.normalizer import resolve_preferred_quote_symbols

        candidates = ["SOLUSDT", "SOLUSDC", "AAVEUSDC", "UNIUSDT", "UNIUSDC", "UNIBTC"]
        selected = resolve_preferred_quote_symbols(candidates, exchange="binance")

        self.assertIn("SOLUSDT", selected)
        self.assertNotIn("SOLUSDC", selected)
        self.assertIn("AAVEUSDC", selected)
        self.assertIn("UNIUSDT", selected)
        self.assertNotIn("UNIUSDC", selected)
        self.assertNotIn("UNIBTC", selected)



if __name__ == "__main__":
    unittest.main()
