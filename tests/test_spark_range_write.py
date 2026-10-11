"""
Benchmark script: Test Spark DataSourceV2 write on partitioned Iceberg table
using TBLPROPERTIES ('write.distribution-mode'='range', 'write.fanout.enabled'='false')
on a real dataset (BTC-USD, 2.26 million rows spanning 1,584 distinct days).
"""

import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def main():
    print("[1/5] Initializing SparkSession...")
    spark = SparkSession.builder \
        .appName("TestSparkRangeWriteBenchmark") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "rest") \
        .config("spark.sql.catalog.lakehouse.uri", "http://rest:8181") \
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", "admin") \
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", "password") \
        .config("spark.sql.defaultCatalog", "lakehouse") \
        .getOrCreate()

    print("[2/5] Creating table test_spark_range with range + fanout.enabled=false...")
    spark.sql("DROP TABLE IF EXISTS lakehouse.bronze.test_spark_range")
    spark.sql("""
    CREATE TABLE lakehouse.bronze.test_spark_range (
        exchange STRING,
        raw_symbol STRING,
        timestamp TIMESTAMP,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        trade_count BIGINT,
        source STRING,
        base_currency STRING,
        quote_currency STRING,
        canonical_symbol STRING,
        batch_id STRING,
        ingested_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (exchange, days(timestamp))
    TBLPROPERTIES ('write.distribution-mode'='range', 'write.fanout.enabled'='false')
    """)

    print("[3/5] Reading canonical BTC-USD staging Parquet files...")
    input_path = "/home/iceberg/warehouse_local/staging/canonical/alpaca/BTC-USD/*.parquet"
    df = spark.read.parquet(input_path).withColumn("ingested_at", current_timestamp())
    count = df.count()
    print(f"[4/5] Successfully read {count:,} rows. Starting Spark writeTo(...).append()...")

    t0 = time.time()
    try:
        df.writeTo("lakehouse.bronze.test_spark_range").append()
        elapsed = time.time() - t0
        print(f"[5/5] SUCCESS! Spark DataSourceV2 wrote {count:,} rows in {elapsed:.2f}s ({count/elapsed:,.0f} rows/s)")
    except Exception as e:
        elapsed = time.time() - t0
        print(f"[5/5] FAILED after {elapsed:.2f}s with error:\n{e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
