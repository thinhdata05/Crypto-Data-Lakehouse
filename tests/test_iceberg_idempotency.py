"""
Integration Test Suite: Iceberg Table Overwrite & Idempotent Replay Contracts.

Validates:
1. Target table atomic overwrite (tbl.overwrite(..., overwrite_filter=...)) produces identical
   row count upon multiple backfill replays (zero duplicate accumulation).
2. Partition-pruned / symbol-scoped overwrite leaves other symbols untouched.
3. Live interaction with Apache Iceberg REST Catalog and MinIO S3 storage.
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
import sys
import unittest

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import DoubleType, NestedField, StringType, TimestampType


def get_test_catalog():
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
        cls.catalog = get_test_catalog()
        cls.table_name = "silver.test_idempotent_replay_contract"

    def setUp(self):
        if not self.catalog:
            self.skipTest("Iceberg REST catalog is not accessible in current environment.")
        # Ensure clean state
        try:
            self.catalog.drop_table(self.table_name)
        except Exception:
            pass

    def tearDown(self):
        if self.catalog:
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


if __name__ == "__main__":
    unittest.main()
