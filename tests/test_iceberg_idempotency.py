"""
Integration Test Suite: Iceberg Table Overwrite, Replay Idempotency & Serving API Contracts.

Validates:
1. Target table atomic overwrite (tbl.overwrite(..., overwrite_filter=...)) produces identical
   row count upon multiple backfill replays (zero duplicate accumulation).
2. Partition-pruned / symbol-scoped overwrite leaves other symbols untouched.
3. Live interaction with Apache Iceberg REST Catalog and MinIO S3 storage.
4. Happy-path verification of query_minute_spread() bounded serving API with partition pushdown.
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
import sys
import unittest

try:
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog
    from pyiceberg.expressions import And, EqualTo
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import DayTransform
    from pyiceberg.types import DoubleType, NestedField, StringType, TimestampType
    HAS_PYICEBERG = True
except ImportError:
    HAS_PYICEBERG = False


def get_test_catalog():
    if not HAS_PYICEBERG:
        return None
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
    s3_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

    try:
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
        cat.list_namespaces()
        return cat
    except Exception:
        return None


class TestIcebergIdempotencyIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not HAS_PYICEBERG:
            return
        cls.catalog = get_test_catalog()
        cls.table_name = "silver.test_idempotent_replay_contract"

    def setUp(self):
        if not HAS_PYICEBERG:
            self.skipTest(
                "pyiceberg is not installed in the local host environment. "
                "Run tests inside Docker: docker exec -w /home/iceberg lakehouse-spark-iceberg python -m unittest discover tests"
            )
        if not self.catalog:
            self.skipTest("Iceberg REST catalog is not accessible in current environment.")
        # Ensure clean state
        try:
            self.catalog.drop_table(self.table_name)
        except Exception:
            pass

    def tearDown(self):
        if HAS_PYICEBERG and self.catalog:
            try:
                self.catalog.drop_table(self.table_name)
            except Exception:
                pass

    def _create_temp_table(self):
        schema = Schema(
            NestedField(1, "exchange", StringType(), required=True),
            NestedField(2, "raw_symbol", StringType(), required=True),
            NestedField(3, "timestamp", TimestampType(), required=True),
            NestedField(4, "close", DoubleType(), required=False),
        )
        spec = PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="timestamp_day")
        )
        return self.catalog.create_table(self.table_name, schema=schema, partition_spec=spec)

    def test_iceberg_table_overwrite_replay_idempotency(self):
        tbl = self._create_temp_table()
        target_schema = tbl.schema().as_arrow()

        ts1 = datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 8, 1, 10, 1, 0, tzinfo=timezone.utc)
        ts3 = datetime(2026, 8, 1, 10, 2, 0, tzinfo=timezone.utc)

        batch_initial = pa.Table.from_pydict(
            {
                "exchange": ["binance", "binance", "binance"],
                "raw_symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
                "timestamp": [ts1, ts2, ts3],
                "close": [50000.0, 50100.0, 50200.0],
            },
            schema=target_schema,
        )

        # 1. First Load: Append
        tbl.append(batch_initial)
        records_run1 = len(tbl.scan().to_arrow())
        self.assertEqual(records_run1, 3, "Initial append should produce exactly 3 records")

        # 2. Replay 1: Overwrite with identical batch
        sym_filter = And(EqualTo("exchange", "binance"), EqualTo("raw_symbol", "BTCUSDT"))
        tbl.overwrite(batch_initial, overwrite_filter=sym_filter)
        records_run2 = len(tbl.scan().to_arrow())
        self.assertEqual(records_run2, 3, "First overwrite replay must maintain exactly 3 records (zero duplicates)")

        # 3. Replay 2: Overwrite again
        tbl.overwrite(batch_initial, overwrite_filter=sym_filter)
        records_run3 = len(tbl.scan().to_arrow())
        self.assertEqual(records_run3, 3, "Second overwrite replay must maintain exactly 3 records")

        # 4. In-Place Update Replay: Overwrite with updated prices
        batch_updated = pa.Table.from_pydict(
            {
                "exchange": ["binance", "binance", "binance"],
                "raw_symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
                "timestamp": [ts1, ts2, ts3],
                "close": [50050.0, 50150.0, 50250.0],
            },
            schema=target_schema,
        )
        tbl.overwrite(batch_updated, overwrite_filter=sym_filter)
        res_arr = tbl.scan().to_arrow()
        self.assertEqual(len(res_arr), 3, "Updated replay must maintain exactly 3 records")
        closes = res_arr["close"].to_pylist()
        self.assertEqual(closes, [50050.0, 50150.0, 50250.0], "Prices must be updated to new values in-place")

    def test_scoped_overwrite_preserves_other_symbols(self):
        tbl = self._create_temp_table()
        target_schema = tbl.schema().as_arrow()

        ts1 = datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 8, 1, 10, 1, 0, tzinfo=timezone.utc)

        btc_batch = pa.Table.from_pydict(
            {
                "exchange": ["binance", "binance"],
                "raw_symbol": ["BTCUSDT", "BTCUSDT"],
                "timestamp": [ts1, ts2],
                "close": [50000.0, 50100.0],
            },
            schema=target_schema,
        )

        eth_batch = pa.Table.from_pydict(
            {
                "exchange": ["binance", "binance"],
                "raw_symbol": ["ETHUSDT", "ETHUSDT"],
                "timestamp": [ts1, ts2],
                "close": [3000.0, 3010.0],
            },
            schema=target_schema,
        )

        tbl.append(btc_batch)
        tbl.append(eth_batch)
        self.assertEqual(len(tbl.scan().to_arrow()), 4, "Should have 4 total records initially")

        # Overwrite only BTCUSDT with updated price
        btc_updated = pa.Table.from_pydict(
            {
                "exchange": ["binance", "binance"],
                "raw_symbol": ["BTCUSDT", "BTCUSDT"],
                "timestamp": [ts1, ts2],
                "close": [55000.0, 55100.0],
            },
            schema=target_schema,
        )
        btc_filter = And(EqualTo("exchange", "binance"), EqualTo("raw_symbol", "BTCUSDT"))
        tbl.overwrite(btc_updated, overwrite_filter=btc_filter)

        # Verify: Total records still 4, ETHUSDT untouched, BTCUSDT updated
        all_res = tbl.scan().to_arrow().to_pandas()
        self.assertEqual(len(all_res), 4)

        eth_rows = all_res[all_res["raw_symbol"] == "ETHUSDT"]
        self.assertEqual(len(eth_rows), 2)
        self.assertEqual(list(eth_rows["close"]), [3000.0, 3010.0], "ETHUSDT records must remain untouched")

        btc_rows = all_res[all_res["raw_symbol"] == "BTCUSDT"]
        self.assertEqual(len(btc_rows), 2)
        self.assertEqual(list(btc_rows["close"]), [55000.0, 55100.0], "BTCUSDT records must be updated")

    def test_query_minute_spread_happy_path_live(self):
        """
        Happy-Path Test: Verifies query_minute_spread() retrieves and aligns
        minute bars for a valid 1-day window against live Iceberg curated gold tables,
        confirming that ISO string timestamp pushdown filtering works accurately.
        """
        from src.serving.query_minute_spread import query_minute_spread

        try:
            gold_tbl = self.catalog.load_table("gold.curated_crypto_bars")
            if not gold_tbl.current_snapshot():
                self.skipTest("gold.curated_crypto_bars has no snapshot")
        except Exception:
            self.skipTest("Cannot connect to gold.curated_crypto_bars")

        # Query 1-day interval
        start_time = "2026-08-10T00:00:00Z"
        end_time = "2026-08-11T00:00:00Z"

        res = query_minute_spread(
            base_currency="BTC",
            start_time=start_time,
            end_time=end_time,
            catalog=self.catalog,
        )

        self.assertGreater(len(res), 0, "Should retrieve aligned minute bars for BTC in the specified 1-day window")

        expected_cols = [
            "base_currency",
            "timestamp",
            "alpaca_close",
            "alpaca_volume",
            "binance_close_raw",
            "binance_volume",
            "binance_quote",
            "usdt_rate_usd",
            "binance_close_usd",
            "spread_usd",
            "spread_pct",
        ]
        for col in expected_cols:
            self.assertIn(col, res.column_names, f"Expected column '{col}' in minute spread result")

        # Assert partition pushdown correctness: all timestamps within [start, end]
        timestamps = res["timestamp"].to_pylist()
        dt_start = datetime.fromisoformat("2026-08-10T00:00:00+00:00")
        dt_end = datetime.fromisoformat("2026-08-11T00:00:00+00:00")
        self.assertTrue(
            all(dt_start <= ts <= dt_end for ts in timestamps),
            "All timestamps must strictly lie within the requested window bounds"
        )


if __name__ == "__main__":
    unittest.main()
