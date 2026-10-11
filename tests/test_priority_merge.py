"""
Unit tests for Priority Merge Resolution Logic according to PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md.
Tests:
1. Intra-exchange priority resolution: (exchange, raw_symbol, timestamp) with 3 > 2 > 1 and ingested_at order.
2. Multi-exchange independence: Alpaca and Binance records for the same minute NEVER collide or overwrite.
"""

from datetime import datetime, timezone
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.preprocessing.normalizer import (
    HISTORICAL_PRIORITY,
    INITIAL_REALTIME_PRIORITY,
    UPDATED_REALTIME_PRIORITY,
)


def simulate_curated_silver_merge(
    target_table: list[dict],
    incoming_updates: list[dict],
) -> list[dict]:
    """
    Simulates the Iceberg MERGE INTO condition on Curated Silver:
    ON target.exchange = updates.exchange
       AND target.raw_symbol = updates.raw_symbol
       AND target.timestamp = updates.timestamp
    WHEN MATCHED AND (
        updates.priority > target.priority
        OR (updates.priority = target.priority AND updates.ingested_at >= target.ingested_at)
    ) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    result = {
        (row["exchange"], row["raw_symbol"], row["timestamp"]): row.copy()
        for row in target_table
    }

    for incoming in incoming_updates:
        key = (incoming["exchange"], incoming["raw_symbol"], incoming["timestamp"])
        if key in result:
            target = result[key]
            if (incoming["priority"] > target["priority"]) or (
                incoming["priority"] == target["priority"]
                and incoming["ingested_at"] >= target["ingested_at"]
            ):
                result[key] = incoming.copy()
        else:
            result[key] = incoming.copy()

    return list(result.values())


class TestPriorityMerge(unittest.TestCase):

    def test_multi_exchange_independence(self):
        """
        Alpaca and Binance have records for the same minute and base currency.
        Both records MUST coexist in Curated Silver! Neither overwrites the other!
        """
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        target = [
            {
                "exchange": "alpaca",
                "raw_symbol": "BAT/USD",
                "timestamp": ts,
                "priority": 2,
                "close": 0.25,
                "ingested_at": 100,
            }
        ]
        incoming = [
            {
                "exchange": "binance",
                "raw_symbol": "BATUSDT",
                "timestamp": ts,
                "priority": 2,
                "close": 0.2505,
                "ingested_at": 101,
            }
        ]

        merged = simulate_curated_silver_merge(target, incoming)
        # Both must exist!
        self.assertEqual(len(merged), 2)
        exchanges = {r["exchange"] for r in merged}
        self.assertEqual(exchanges, {"alpaca", "binance"})

    def test_intra_exchange_priority_resolution(self):
        """
        Within the SAME exchange (alpaca), higher priority wins (3 > 2 > 1).
        """
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        target = [
            {
                "exchange": "alpaca",
                "raw_symbol": "BTC/USD",
                "timestamp": ts,
                "priority": 1,
                "close": 50000.0,
                "ingested_at": 100,
            }
        ]
        # Historical bar arrives (priority 2)
        incoming_hist = [
            {
                "exchange": "alpaca",
                "raw_symbol": "BTC/USD",
                "timestamp": ts,
                "priority": 2,
                "close": 50005.0,
                "ingested_at": 110,
            }
        ]
        merged = simulate_curated_silver_merge(target, incoming_hist)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["priority"], 2)
        self.assertEqual(merged[0]["close"], 50005.0)

        # Updated realtime bar arrives (priority 3)
        incoming_update = [
            {
                "exchange": "alpaca",
                "raw_symbol": "BTC/USD",
                "timestamp": ts,
                "priority": 3,
                "close": 50010.0,
                "ingested_at": 120,
            }
        ]
        merged2 = simulate_curated_silver_merge(merged, incoming_update)
        self.assertEqual(len(merged2), 1)
        self.assertEqual(merged2[0]["priority"], 3)
        self.assertEqual(merged2[0]["close"], 50010.0)

    def test_lower_priority_ignored(self):
        """
        Lower priority cannot overwrite higher priority within the same exchange.
        """
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        target = [
            {
                "exchange": "alpaca",
                "raw_symbol": "BTC/USD",
                "timestamp": ts,
                "priority": 3,
                "close": 50010.0,
                "ingested_at": 200,
            }
        ]
        late_incoming = [
            {
                "exchange": "alpaca",
                "raw_symbol": "BTC/USD",
                "timestamp": ts,
                "priority": 2,
                "close": 50005.0,
                "ingested_at": 250,
            }
        ]
        merged = simulate_curated_silver_merge(target, late_incoming)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["priority"], 3)
        self.assertEqual(merged[0]["close"], 50010.0)


if __name__ == "__main__":
    unittest.main()
