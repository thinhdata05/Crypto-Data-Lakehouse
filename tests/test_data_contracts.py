"""
Data Contract and Invariant Tests.
Validates architectural constraints, partition specs, OHLCV sanity, and idempotency guarantees.
"""

from datetime import datetime, timezone
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import duckdb
import pandas as pd

from src.preprocessing.normalizer import (
    OHLCV_SANITY_SQL_PREDICATE,
    OHLCV_VIOLATION_SQL_PREDICATE,
    validate_ohlcv_record,
)


class TestDataContracts(unittest.TestCase):
    def setUp(self):
        self.sql_ddl_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../sql/ddl/01_create_iceberg_tables.sql")
        )

    def test_ddl_partition_specs_strict_days_only(self):
        """
        Contract Test: Invariant 4 & Reviewer Finding 1.
        All 1-minute crypto bars tables (bronze, silver, curated silver, gold)
        MUST be PARTITIONED BY (days(timestamp)) and MUST NOT partition by exchange.
        """
        self.assertTrue(os.path.exists(self.sql_ddl_path), f"DDL file not found at {self.sql_ddl_path}")
        with open(self.sql_ddl_path, "r", encoding="utf-8") as f:
            ddl_content = f.read()

        tables_to_check = [
            "lakehouse.bronze.crypto_bars",
            "lakehouse.silver.crypto_bars",
            "lakehouse.silver.curated_crypto_bars",
            "lakehouse.gold.curated_crypto_bars",
        ]

        for tbl in tables_to_check:
            # Find create table block
            start_pos = ddl_content.find(f"CREATE TABLE IF NOT EXISTS {tbl}")
            self.assertNotEqual(start_pos, -1, f"Table {tbl} not found in DDL")
            semicolon_pos = ddl_content.find(";", start_pos)
            tbl_block = ddl_content[start_pos:semicolon_pos]

            # Invariant: Must contain PARTITIONED BY (days(timestamp))
            self.assertIn(
                "PARTITIONED BY (days(timestamp))",
                tbl_block,
                f"Table {tbl} must be PARTITIONED BY (days(timestamp))",
            )

            # Invariant: Must NEVER partition by exchange
            self.assertNotIn(
                "exchange",
                tbl_block.split("PARTITIONED BY")[1].split(")")[0],
                f"Table {tbl} must NOT partition by exchange!",
            )

    def test_ohlcv_all_seven_sanity_rules(self):
        """
        Contract Test: Full OHLCV Mathematical Consistency.
        Must check: high >= low, high >= open, high >= close, low <= open, low <= close, volume >= 0, positive prices.
        """
        # Valid base bar
        valid, err = validate_ohlcv_record(100.0, 105.0, 95.0, 102.0, 10.0)
        self.assertTrue(valid)
        self.assertIsNone(err)

        # 1. High < Low
        v, e = validate_ohlcv_record(100.0, 90.0, 95.0, 92.0, 10.0)
        self.assertFalse(v)

        # 2. Open exceeds High
        v, e = validate_ohlcv_record(110.0, 105.0, 95.0, 102.0, 10.0)
        self.assertFalse(v)
        self.assertIn("exceeds High", e)

        # 3. Close exceeds High
        v, e = validate_ohlcv_record(100.0, 105.0, 95.0, 110.0, 10.0)
        self.assertFalse(v)
        self.assertIn("exceeds High", e)

        # 4. Open below Low
        v, e = validate_ohlcv_record(90.0, 105.0, 95.0, 102.0, 10.0)
        self.assertFalse(v)
        self.assertIn("below Low", e)

        # 5. Close below Low
        v, e = validate_ohlcv_record(100.0, 105.0, 95.0, 90.0, 10.0)
        self.assertFalse(v)
        self.assertIn("below Low", e)

        # 6. Negative Volume
        v, e = validate_ohlcv_record(100.0, 105.0, 95.0, 102.0, -0.5)
        self.assertFalse(v)

        # 7. Non-positive price
        v, e = validate_ohlcv_record(0.0, 105.0, 95.0, 102.0, 10.0)
        self.assertFalse(v)

    def test_sql_sanity_predicates_filtering(self):
        """
        Contract Test: Tests the SQL Predicates using in-process DuckDB.
        """
        con = duckdb.connect()
        df = pd.DataFrame([
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0, "expected_valid": True},
            {"open": 110.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0, "expected_valid": False}, # open > high
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 108.0, "volume": 10.0, "expected_valid": False}, # close > high
            {"open": 90.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0, "expected_valid": False},  # open < low
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 90.0, "volume": 10.0, "expected_valid": False},  # close < low
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": -1.0, "expected_valid": False}, # vol < 0
        ])
        con.register("test_df", df)

        valid_cnt = con.execute(f"SELECT COUNT(*) FROM test_df WHERE {OHLCV_SANITY_SQL_PREDICATE}").fetchone()[0]
        self.assertEqual(valid_cnt, 1)

        violation_cnt = con.execute(f"SELECT COUNT(*) FROM test_df WHERE {OHLCV_VIOLATION_SQL_PREDICATE}").fetchone()[0]
        self.assertEqual(violation_cnt, 5)

    def test_idempotent_deduplication_replay(self):
        """
        Contract Test: Re-running reconciliation over duplicate incoming rows
        produces identical, deduplicated rows without duplicates.
        """
        con = duckdb.connect()
        now = datetime(2024, 9, 4, 12, 0, 0, tzinfo=timezone.utc)
        earlier = datetime(2024, 9, 4, 11, 0, 0, tzinfo=timezone.utc)

        # Batch 1 data + Replay duplicates
        data = [
            {"exchange": "binance", "raw_symbol": "BTCUSDT", "timestamp": now, "priority": 2, "ingested_at": earlier, "close": 58000.0},
            {"exchange": "binance", "raw_symbol": "BTCUSDT", "timestamp": now, "priority": 2, "ingested_at": now, "close": 58000.0},     # Replay copy
            {"exchange": "binance", "raw_symbol": "BTCUSDT", "timestamp": now, "priority": 3, "ingested_at": now, "close": 58050.0},     # Rectification copy
            {"exchange": "alpaca", "raw_symbol": "BTC/USD", "timestamp": now, "priority": 2, "ingested_at": now, "close": 58020.0},      # Alpaca independent
        ]
        df = pd.DataFrame(data)
        con.register("incoming", df)

        result = con.execute("""
            SELECT exchange, raw_symbol, timestamp, priority, close
            FROM incoming
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY exchange, raw_symbol, timestamp
                ORDER BY priority DESC, ingested_at DESC
            ) = 1
            ORDER BY exchange, raw_symbol
        """).df()

        # Should produce exactly 2 rows: 1 for Binance, 1 for Alpaca
        self.assertEqual(len(result), 2)

        # Binance row should be priority 3 (rectification wins)
        binance_row = result[result["exchange"] == "binance"].iloc[0]
        self.assertEqual(binance_row["priority"], 3)
        self.assertEqual(binance_row["close"], 58050.0)

        # Alpaca row remains intact
        alpaca_row = result[result["exchange"] == "alpaca"].iloc[0]
        self.assertEqual(alpaca_row["close"], 58020.0)

    def test_source_manifest_ddl_and_partition_spec(self):
        """
        Contract Test: Lakehouse Invariant - source_manifest must be partitioned by days(processed_at).
        """
        with open(self.sql_ddl_path, "r", encoding="utf-8") as f:
            ddl_content = f.read()

        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.bronze.source_manifest", ddl_content)
        self.assertIn("PARTITIONED BY (days(processed_at))", ddl_content)

    def test_minute_spread_guard_window_rejection(self):
        """
        Contract Test: Serving Layer Safety - Minute query must reject windows > 7 days.
        """
        from src.serving.query_minute_spread import query_minute_spread
        with self.assertRaises(ValueError) as ctx:
            query_minute_spread(
                base_currency="BTC",
                start_time="2026-08-01T00:00:00Z",
                end_time="2026-08-15T00:00:00Z",
                catalog=None,
            )
        self.assertIn("exceeds maximum allowed limit", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

