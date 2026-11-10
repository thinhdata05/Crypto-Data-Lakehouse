"""
Data Contract and Invariant Tests.
Validates architectural constraints, partition specs, OHLCV sanity, and idempotency guarantees.
"""

from datetime import datetime, timezone
import hashlib
import json
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
        All 1-minute crypto bars tables across Local (bronze.crypto_bars, silver.bars)
        MUST be PARTITIONED BY (days(timestamp)) and MUST NOT partition by exchange.
        """
        self.assertTrue(os.path.exists(self.sql_ddl_path), f"DDL file not found at {self.sql_ddl_path}")
        with open(self.sql_ddl_path, "r", encoding="utf-8") as f:
            local_ddl = f.read()

        # Invariant: Local DDL must NOT create lakehouse.gold
        self.assertNotIn("CREATE DATABASE IF NOT EXISTS lakehouse.gold", local_ddl)
        self.assertNotIn("lakehouse.gold.curated_crypto_bars", local_ddl)

        tables_to_check = [
            "lakehouse.bronze.crypto_bars",
            "lakehouse.silver.bars",
        ]

        for tbl in tables_to_check:
            # Find create table block
            start_pos = local_ddl.find(f"CREATE TABLE IF NOT EXISTS {tbl}")
            self.assertNotEqual(start_pos, -1, f"Table {tbl} not found in DDL")
            semicolon_pos = local_ddl.find(";", start_pos)
            tbl_block = local_ddl[start_pos:semicolon_pos]

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

        # 8. Negative trade count
        v, e = validate_ohlcv_record(100.0, 105.0, 95.0, 102.0, 10.0, trade_count_val=-1)
        self.assertFalse(v)
        self.assertIn("Negative trade count", e)

    def test_sql_sanity_predicates_filtering(self):
        """
        Contract Test: Tests the SQL Predicates using in-process DuckDB.
        """
        con = duckdb.connect()
        df = pd.DataFrame([
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0, "trade_count": 5, "expected_valid": True},
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0, "trade_count": None, "expected_valid": True}, # nullable trade_count ok
            {"open": 110.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0, "trade_count": 1, "expected_valid": False}, # open > high
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 108.0, "volume": 10.0, "trade_count": 1, "expected_valid": False}, # close > high
            {"open": 90.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0, "trade_count": 1, "expected_valid": False},  # open < low
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 90.0, "volume": 10.0, "trade_count": 1, "expected_valid": False},  # close < low
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": -1.0, "trade_count": 1, "expected_valid": False}, # vol < 0
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0, "trade_count": -5, "expected_valid": False}, # trade_count < 0
        ])
        con.register("test_df", df)

        valid_cnt = con.execute(f"SELECT COUNT(*) FROM test_df WHERE {OHLCV_SANITY_SQL_PREDICATE}").fetchone()[0]
        self.assertEqual(valid_cnt, 2)

        violation_cnt = con.execute(f"SELECT COUNT(*) FROM test_df WHERE {OHLCV_VIOLATION_SQL_PREDICATE}").fetchone()[0]
        self.assertEqual(violation_cnt, 6)

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
        Contract Test: Lakehouse Control Plane Invariant - ops.ingestion_manifest and ops.pipeline_audit
        must live in the ops namespace and be partitioned by days(processed_at) and days(created_at).
        """
        with open(self.sql_ddl_path, "r", encoding="utf-8") as f:
            ddl_content = f.read()

        self.assertIn("CREATE DATABASE IF NOT EXISTS lakehouse.ops", ddl_content)
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.ops.ingestion_manifest", ddl_content)
        self.assertIn("PARTITIONED BY (days(processed_at))", ddl_content)
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.ops.pipeline_audit", ddl_content)
        self.assertIn("PARTITIONED BY (days(created_at))", ddl_content)

    def test_minute_spread_guard_window_rejection(self):
        """
        Contract Test: Serving Layer Safety - Minute query must reject windows > 7 days.
        """
        try:
            from src.serving.query_minute_spread import query_minute_spread
        except ImportError:
            self.skipTest("pyiceberg is not installed in the local host environment. Run tests inside Docker container.")

        with self.assertRaises(ValueError) as ctx:
            query_minute_spread(
                base_currency="BTC",
                start_time="2026-08-01T00:00:00Z",
                end_time="2026-08-15T00:00:00Z",
                catalog=None,
            )
        self.assertIn("exceeds maximum allowed limit", str(ctx.exception))

    def test_dynamic_priority_resolution_in_silver(self):
        """
        Contract Test: Dynamic Priority Assignment (3 > 2 > 1) in Silver.
        Ensures Bronze -> Silver no longer hardcodes priority=2 and properly resolves
        realtime (1), historical/REST (2), and exchange correction (3) within the same exchange.
        """
        from src.preprocessing.normalizer import (
            HISTORICAL_PRIORITY,
            INITIAL_REALTIME_PRIORITY,
            SOURCE_TO_PRIORITY_SQL_CASE,
            UPDATED_REALTIME_PRIORITY,
            resolve_priority_from_source,
        )

        self.assertEqual(resolve_priority_from_source("realtime"), INITIAL_REALTIME_PRIORITY)
        self.assertEqual(resolve_priority_from_source("b"), INITIAL_REALTIME_PRIORITY)
        self.assertEqual(resolve_priority_from_source("historical"), HISTORICAL_PRIORITY)
        self.assertEqual(resolve_priority_from_source("rest"), HISTORICAL_PRIORITY)
        self.assertEqual(resolve_priority_from_source("correction"), UPDATED_REALTIME_PRIORITY)
        self.assertEqual(resolve_priority_from_source("u"), UPDATED_REALTIME_PRIORITY)

        con = duckdb.connect()
        now = datetime(2026, 11, 4, 15, 0, 0, tzinfo=timezone.utc)
        df = pd.DataFrame([
            {"exchange": "alpaca", "raw_symbol": "BTC/USD", "timestamp": now, "source": "realtime", "ingested_at": now, "close": 70000.0},
            {"exchange": "alpaca", "raw_symbol": "BTC/USD", "timestamp": now, "source": "historical", "ingested_at": now, "close": 70050.0},
            {"exchange": "alpaca", "raw_symbol": "BTC/USD", "timestamp": now, "source": "correction", "ingested_at": now, "close": 70100.0},
            {"exchange": "binance", "raw_symbol": "BTCUSDT", "timestamp": now, "source": "historical", "ingested_at": now, "close": 70080.0},
        ])
        con.register("raw_arr", df)
        res = con.execute(f"""
            WITH ranked AS (
                SELECT
                    exchange,
                    raw_symbol,
                    timestamp,
                    source,
                    {SOURCE_TO_PRIORITY_SQL_CASE} AS priority,
                    ingested_at,
                    close
                FROM raw_arr
            )
            SELECT exchange, raw_symbol, priority, close
            FROM ranked
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY exchange, raw_symbol, timestamp
                ORDER BY priority DESC, ingested_at DESC
            ) = 1
            ORDER BY exchange
        """).df()

        self.assertEqual(len(res), 2)
        alpaca_row = res[res["exchange"] == "alpaca"].iloc[0]
        self.assertEqual(int(alpaca_row["priority"]), 3)
        self.assertEqual(float(alpaca_row["close"]), 70100.0)

        binance_row = res[res["exchange"] == "binance"].iloc[0]
        self.assertEqual(int(binance_row["priority"]), 2)
        self.assertEqual(float(binance_row["close"]), 70080.0)

    def test_publish_confirmed_parquet_and_manifest_contract(self):
        """
        Contract Test: Phase 2 Hybrid Producer (`publish_confirmed_slice`).
        Verifies immutable per-exchange Parquet generation, SHA-256 checksums in `_manifest.json`,
        priority=2 preservation, and idempotent replay.
        """
        import shutil
        import tempfile
        import pyarrow as pa
        from src.pipeline.incremental.publish_confirmed_parquet import (
            compute_file_sha256,
            publish_confirmed_slice,
        )

        tmp_dir = tempfile.mkdtemp(prefix="test_confirmed_")
        try:
            ts = datetime(2026, 11, 4, 14, 0, 0, tzinfo=timezone.utc)
            arr = pa.Table.from_pydict({
                "exchange": ["alpaca", "binance"],
                "raw_symbol": ["BTC/USD", "BTCUSDT"],
                "timestamp": [ts, ts],
                "open": [70000.0, 70010.0],
                "high": [70100.0, 70120.0],
                "low": [69900.0, 69950.0],
                "close": [70050.0, 70080.0],
                "volume": [12.5, 45.2],
                "trade_count": [120, 540],
                "source": ["historical", "historical"],
                "priority": [2, 2],
                "base_currency": ["BTC", "BTC"],
                "quote_currency": ["USD", "USDT"],
                "canonical_symbol": ["BTC-USD", "BTC-USDT"],
                "batch_id": ["batch123", "batch123"],
                "ingested_at": [ts, ts],
            })

            manifest_1 = publish_confirmed_slice(
                arrow_table=arr,
                batch_id="batch123",
                window_start="2026-11-04T14:00:00Z",
                window_end="2026-11-04T15:00:00Z",
                output_base_dir=tmp_dir,
                s3_base_uri=None,
            )

            self.assertEqual(manifest_1["batch_id"], "batch123")
            self.assertEqual(manifest_1["total_records"], 2)
            self.assertEqual(len(manifest_1["files"]), 2)

            batch_dir = os.path.join(tmp_dir, "batch_id=batch123")
            self.assertTrue(os.path.exists(os.path.join(batch_dir, "_manifest.json")))

            for f_entry in manifest_1["files"]:
                p_file = os.path.join(batch_dir, f_entry["filename"])
                self.assertTrue(os.path.exists(p_file))
                self.assertEqual(compute_file_sha256(p_file), f_entry["sha256"])
                self.assertEqual(f_entry["priority"], 2)
                self.assertEqual(f_entry["record_count"], 1)

            # Replay idempotency: second call must return identical manifest without corrupting files
            manifest_2 = publish_confirmed_slice(
                arrow_table=arr,
                batch_id="batch123",
                window_start="2026-11-04T14:00:00Z",
                window_end="2026-11-04T15:00:00Z",
                output_base_dir=tmp_dir,
                s3_base_uri=None,
            )
            self.assertEqual(manifest_1["files"], manifest_2["files"])
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_option1_local_lightweight_producer_skips_local_gold_in_hourly_dag(self):
        """
        Contract Test: Option 1 (Anti-Split-Brain).
        Local hourly incremental pipeline MUST stop at Curated Silver and publish confirmed/ Parquets,
        skipping Local Gold materialization by default.
        """
        import inspect
        try:
            from src.pipeline.incremental.incremental_sync import run_duckdb_incremental_sync
            sig = inspect.signature(run_duckdb_incremental_sync)
            self.assertIn("sync_gold", sig.parameters)
            self.assertFalse(
                sig.parameters["sync_gold"].default,
                "run_duckdb_incremental_sync must default to sync_gold=False under Option 1!",
            )
        except ImportError as e:
            pass  # pyiceberg not installed on Windows host, verified inside Docker container

        dag_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../airflow/dags/lakehouse_incremental_hourly_dag.py")
        )
        if os.path.exists(dag_path):
            with open(dag_path, "r", encoding="utf-8") as f:
                dag_code = f.read()

            self.assertNotIn(
                "materialize_gold_aggregates.py",
                dag_code,
                "Hourly local DAG must not materialize local Gold aggregates under Option 1!",
            )
            self.assertIn("publish_confirmed_parquet.py", dag_code)
            self.assertIn("--target-table silver.bars", dag_code)

    def test_trade_count_nullable_contract(self):
        """
        Contract Test: trade_count Nullable & Non-negative Contract.
        Verifies that trade_count can be NULL (for feeds lacking trade counts) or >= 0,
        and strictly rejects negative trade counts.
        """
        # 1. Python validation checks
        self.assertTrue(validate_ohlcv_record(100.0, 105.0, 95.0, 102.0, 10.0, trade_count_val=None)[0])
        self.assertTrue(validate_ohlcv_record(100.0, 105.0, 95.0, 102.0, 10.0, trade_count_val=0)[0])
        self.assertTrue(validate_ohlcv_record(100.0, 105.0, 95.0, 102.0, 10.0, trade_count_val=15)[0])

        is_valid, err = validate_ohlcv_record(100.0, 105.0, 95.0, 102.0, 10.0, trade_count_val=-1)
        self.assertFalse(is_valid)
        self.assertIn("Negative trade count", err)

        # 2. SQL Predicate checks via DuckDB
        con = duckdb.connect()
        df = pd.DataFrame([
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0, "trade_count": None},
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0, "trade_count": 0},
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0, "trade_count": 50},
            {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0, "trade_count": -10},
        ])
        con.register("trade_count_test", df)

        valid_rows = con.execute(f"SELECT COUNT(*) FROM trade_count_test WHERE {OHLCV_SANITY_SQL_PREDICATE}").fetchone()[0]
        self.assertEqual(valid_rows, 3)  # NULL, 0, 50 accepted

        violation_rows = con.execute(f"SELECT COUNT(*) FROM trade_count_test WHERE {OHLCV_VIOLATION_SQL_PREDICATE}").fetchone()[0]
        self.assertEqual(violation_rows, 1)  # -10 rejected

    def test_atomic_manifest_and_s3_upload_contract(self):
        """
        Contract Test: Atomic Manifest, Verified Checksum, and Retry Recovery.
        Verifies:
        1. Local atomic file rename (_manifest.json.tmp -> _manifest.json).
        2. Parquet data uploaded first and byte checksums verified against remote S3 object.
        3. _manifest.json uploaded LAST as the final remote commit marker.
        4. Crash/retry recovery: if local files are valid but remote S3 is incomplete,
           retry does NOT prematurely skip, but resumes S3 upload and completes the batch.
        """
        import shutil
        import tempfile
        import pyarrow as pa
        from pyarrow.fs import S3FileSystem, FileType
        from src.pipeline.incremental.publish_confirmed_parquet import (
            publish_confirmed_slice,
            write_json_atomic,
            compute_file_sha256,
        )

        tmp_dir = tempfile.mkdtemp(prefix="test_s3_pub_")
        try:
            # 1. Test atomic write helper
            test_json_path = os.path.join(tmp_dir, "test.json")
            write_json_atomic(test_json_path, {"status": "ok", "version": 1})
            self.assertTrue(os.path.exists(test_json_path))
            with open(test_json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.assertEqual(data["status"], "ok")

            # 2. Test live S3 upload sequence using MinIO
            endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
            clean_endpoint = endpoint.replace("http://", "").replace("https://", "")
            s3_fs = S3FileSystem(
                access_key=os.getenv("AWS_ACCESS_KEY_ID", "admin"),
                secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
                endpoint_override=clean_endpoint,
                scheme="http",
                force_virtual_addressing=False,
            )
            try:
                s3_fs.get_file_info("lakehouse")
            except Exception as e:
                self.skipTest(f"Live S3/MinIO endpoint not available on host: {e}")

            ts = datetime(2026, 11, 4, 15, 0, 0, tzinfo=timezone.utc)
            arr = pa.Table.from_pydict({
                "exchange": ["alpaca"],
                "raw_symbol": ["ETH/USD"],
                "timestamp": [ts],
                "open": [3500.0],
                "high": [3550.0],
                "low": [3490.0],
                "close": [3520.0],
                "volume": [20.0],
                "trade_count": [150],
                "source": ["historical"],
                "priority": [2],
                "base_currency": ["ETH"],
                "quote_currency": ["USD"],
                "canonical_symbol": ["ETH-USD"],
                "batch_id": ["batch_s3_test_01"],
                "ingested_at": [ts],
            })

            batch_id = "batch_s3_test_01"
            manifest = publish_confirmed_slice(
                arrow_table=arr,
                batch_id=batch_id,
                window_start="2026-11-04T15:00:00Z",
                window_end="2026-11-04T16:00:00Z",
                output_base_dir=tmp_dir,
                s3_base_uri="s3://lakehouse/staging_confirmed_test",
                upload_to_s3=True,
            )

            batch_dir = os.path.join(tmp_dir, f"batch_id={batch_id}")
            local_parquet_path = os.path.join(batch_dir, "alpaca_bars.parquet")
            local_manifest_path = os.path.join(batch_dir, "_manifest.json")
            s3_parquet_key = f"lakehouse/staging_confirmed_test/batch_id={batch_id}/alpaca_bars.parquet"
            s3_manifest_key = f"lakehouse/staging_confirmed_test/batch_id={batch_id}/_manifest.json"

            # Check that remote files exist on MinIO
            p_info = s3_fs.get_file_info(s3_parquet_key)
            self.assertEqual(p_info.type, FileType.File)
            self.assertEqual(p_info.size, os.path.getsize(local_parquet_path))

            # Download remote object from MinIO and verify SHA-256 byte checksum
            h = hashlib.sha256()
            with s3_fs.open_input_stream(s3_parquet_key) as s3_in:
                while True:
                    chunk = s3_in.read(1024 * 1024)
                    if not chunk:
                        break
                    h.update(chunk)
            remote_parquet_sha256 = h.hexdigest()
            self.assertEqual(remote_parquet_sha256, compute_file_sha256(local_parquet_path))

            # Verify manifest content matching
            with s3_fs.open_input_stream(s3_manifest_key) as m_in:
                remote_manifest_data = json.loads(m_in.read().decode("utf-8"))
            with open(local_manifest_path, "r", encoding="utf-8") as f_in:
                local_manifest_data = json.load(f_in)
            self.assertEqual(remote_manifest_data["batch_id"], local_manifest_data["batch_id"])
            self.assertEqual(remote_manifest_data["files"][0]["sha256"], remote_parquet_sha256)

            # 3. Test Retry / Recovery:
            # Simulate a scenario where remote _manifest.json was missing (e.g. upload interrupted).
            # Delete remote _manifest.json from S3 while keeping local files intact.
            s3_fs.delete_file(s3_manifest_key)
            m_deleted_info = s3_fs.get_file_info(s3_manifest_key)
            self.assertEqual(m_deleted_info.type, FileType.NotFound)

            # Re-running publish_confirmed_slice must detect remote is incomplete and resume upload,
            # NOT return premature local skip!
            resumed_manifest = publish_confirmed_slice(
                arrow_table=arr,
                batch_id=batch_id,
                window_start="2026-11-04T15:00:00Z",
                window_end="2026-11-04T16:00:00Z",
                output_base_dir=tmp_dir,
                s3_base_uri="s3://lakehouse/staging_confirmed_test",
                upload_to_s3=True,
            )
            # Verify remote manifest is now re-created and committed
            m_resumed_info = s3_fs.get_file_info(s3_manifest_key)
            self.assertEqual(m_resumed_info.type, FileType.File)
            self.assertGreater(m_resumed_info.size, 0)

            # Cleanup test keys on MinIO
            s3_fs.delete_file(s3_parquet_key)
            s3_fs.delete_file(s3_manifest_key)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()

