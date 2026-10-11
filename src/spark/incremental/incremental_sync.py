"""
Spark Incremental Sync Job: Hourly window sync and correction reconciliation.
Reads incremental changes from Silver within the specified time window,
applies intra-exchange priority resolution (including late updates T='u' with priority=3),
merges into Curated Silver and Curated Gold on (exchange, raw_symbol, timestamp).
Preserves multi-exchange independence (Alpaca and Binance records are never merged together).
In accordance with PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
import sys
import uuid

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, current_timestamp, row_number

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from src.spark.common.iceberg_session import get_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hourly Incremental Iceberg Sync")
    parser.add_argument("--window-start", required=True, help="ISO start timestamp")
    parser.add_argument("--window-end", required=True, help="ISO end timestamp")
    parser.add_argument("--batch-id", default=None, help="Batch UUID")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    batch_id = args.batch_id or str(uuid.uuid4())
    w_start = args.window_start
    w_end = args.window_end

    print(f"[{datetime.now(timezone.utc).isoformat()}] Incremental Sync Window: {w_start} to {w_end}")

    spark = get_spark_session(app_name="HourlyIncrementalSync")

    try:
        # Check Silver table for window records or batch_id
        if batch_id:
            silver_df = spark.table("lakehouse.silver.crypto_bars").filter(
                (col("batch_id") == batch_id) | ((col("timestamp") >= w_start) & (col("timestamp") < w_end))
            )
        else:
            silver_df = spark.table("lakehouse.silver.crypto_bars").filter(
                (col("timestamp") >= w_start) & (col("timestamp") < w_end)
            )

        cnt = silver_df.count()
        print(f"Found {cnt} records in window from Silver.")

        if cnt > 0:
            # Intra-batch deduplication on (exchange, raw_symbol, timestamp)
            batch_window = Window.partitionBy(
                "exchange", "raw_symbol", "timestamp"
            ).orderBy(
                col("priority").desc(),
                col("ingested_at").desc(),
            )
            incoming_dedup_df = (
                silver_df.withColumn("rn", row_number().over(batch_window))
                .filter(col("rn") == 1)
                .drop("rn")
            )
            incoming_dedup_df.createOrReplaceTempView("incremental_updates")

            # 1. Merge into Curated Silver
            spark.sql("""
            MERGE INTO lakehouse.silver.curated_crypto_bars AS target
            USING incremental_updates AS updates
            ON target.exchange = updates.exchange
               AND target.raw_symbol = updates.raw_symbol
               AND target.timestamp = updates.timestamp
            WHEN MATCHED AND (
                updates.priority > target.priority
                OR (updates.priority = target.priority AND updates.ingested_at >= target.ingested_at)
            ) THEN
              UPDATE SET
                open = updates.open,
                high = updates.high,
                low = updates.low,
                close = updates.close,
                volume = updates.volume,
                trade_count = updates.trade_count,
                source = updates.source,
                priority = updates.priority,
                base_currency = updates.base_currency,
                quote_currency = updates.quote_currency,
                canonical_symbol = updates.canonical_symbol,
                batch_id = updates.batch_id,
                ingested_at = updates.ingested_at
            WHEN NOT MATCHED THEN
              INSERT (
                exchange, raw_symbol, timestamp, open, high, low, close, volume,
                trade_count, source, priority, base_currency, quote_currency,
                canonical_symbol, batch_id, ingested_at
              )
              VALUES (
                updates.exchange, updates.raw_symbol, updates.timestamp, updates.open,
                updates.high, updates.low, updates.close, updates.volume,
                updates.trade_count, updates.source, updates.priority,
                updates.base_currency, updates.quote_currency, updates.canonical_symbol,
                updates.batch_id, updates.ingested_at
              )
            """)
            print("Successfully reconciled incremental records into Curated Silver.")

            # 2. Merge into Curated Gold
            spark.sql("""
            MERGE INTO lakehouse.gold.curated_crypto_bars AS target
            USING incremental_updates AS updates
            ON target.exchange = updates.exchange
               AND target.raw_symbol = updates.raw_symbol
               AND target.timestamp = updates.timestamp
            WHEN MATCHED AND (
                updates.priority > target.priority
                OR (updates.priority = target.priority AND updates.ingested_at >= target.last_updated_at)
            ) THEN
              UPDATE SET
                open = updates.open,
                high = updates.high,
                low = updates.low,
                close = updates.close,
                volume = updates.volume,
                trade_count = updates.trade_count,
                source = updates.source,
                priority = updates.priority,
                base_currency = updates.base_currency,
                quote_currency = updates.quote_currency,
                canonical_symbol = updates.canonical_symbol,
                batch_id = updates.batch_id,
                last_updated_at = current_timestamp()
            WHEN NOT MATCHED THEN
              INSERT (
                exchange, raw_symbol, timestamp, open, high, low, close, volume,
                trade_count, source, priority, base_currency, quote_currency,
                canonical_symbol, batch_id, last_updated_at
              )
              VALUES (
                updates.exchange, updates.raw_symbol, updates.timestamp, updates.open,
                updates.high, updates.low, updates.close, updates.volume,
                updates.trade_count, updates.source, updates.priority,
                updates.base_currency, updates.quote_currency, updates.canonical_symbol,
                updates.batch_id, current_timestamp()
              )
            """)
            print(f"Successfully reconciled {cnt} incremental records into Curated Gold.")

        spark.sql(f"""
        INSERT INTO lakehouse.gold.pipeline_audit
        VALUES ('{batch_id}', 'lakehouse_incremental_hourly', 'incremental_sync', 'SUCCESS', {cnt}, 'Incremental window {w_start} to {w_end}', current_timestamp())
        """)

    except Exception as e:
        print(f"Error in incremental sync: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
