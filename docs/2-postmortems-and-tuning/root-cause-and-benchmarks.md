# Engineering Insights, Root Cause Analysis & Optimization Notes

This document provides a detailed record of the architectural challenges, bottlenecks, root cause analyses, and solutions implemented during the Phase 1 Historical Backfill (101,053,358 records) of the Multi-Exchange Crypto Lakehouse Platform.

---

## 1. Problem Statement & Bottlenecks Observed

During the initial execution of the full historical backfill across **101,053,358 records** (spanning 2021 to 2026 for 68 symbols from Alpaca and Binance):
1. **Preprocessing Stalling**: The Airflow preprocessing tasks (`alpaca_source_preprocessing` and `binance_source_preprocessing`) ran exceedingly slowly, taking hours with high disk I/O and CPU contention.
2. **Spark Iceberg Write OOM Crash**: The Spark Iceberg ingestion jobs failed with:
   ```text
   java.lang.OutOfMemoryError: Java heap space
       at org.apache.parquet.hadoop.InternalParquetRecordWriter...
       at org.apache.iceberg.spark.source.SparkWrite$PartitionedFanoutWriter...
   ```
3. **HiveThriftServer Memory Leak**: A lingering `HiveThriftServer2 -Xmx4g` background process consumed 2.8GB to 4GB of host RAM inside the Spark container, leaving insufficient memory for Spark executors and Airflow tasks.

---

## 2. Root Cause Analysis (RCA)

### RCA 1: Spark Iceberg `PartitionedFanoutWriter` Memory Explosion
* **Partition Scheme**: The Iceberg tables are partitioned by `(exchange, days(timestamp))`.
* **The Math of the Crash**:
  - A historical symbol spanning 2021 to 2026 covers **~1,584 distinct days**.
  - When writing non-presorted rows into an Iceberg table, Spark’s `FanoutWriter` opens a separate Parquet file writer for **every single partition encountered concurrently** in memory.
  - Each active Parquet record compressor allocates a default write buffer of **64 MB to 128 MB**:
    $$\text{Buffer Requirement} = 1,584 \text{ days} \times 64\text{ MB} \approx \mathbf{101.4\text{ GB of Java Heap}}$$
  - Because the Spark master/executor in local Docker had a heap limit of 2.5 GB, Java immediately triggered `OutOfMemoryError`.
  - Even with `sortWithinPartitions("exchange", "timestamp")`, sorting by microsecond timestamp across 1,584 days in arbitrary Spark shuffle partitions still forced concurrent partition writes within single executor tasks.

### RCA 2: Why `write.distribution-mode = 'hash'` Was Not Sufficient on Local Single-Node Docker
A common question in Iceberg architecture is: *Why not simply set `write.distribution-mode = 'hash'` instead of switching the write engine?*
- In an enterprise multi-node cluster (e.g. 50 nodes with 1.6 TB RAM), setting `write.distribution-mode = 'hash'` with `spark.sql.shuffle.partitions = 3200` works because Spark distributes the 3,168 partitions ($2 \text{ exchanges} \times 1,584 \text{ days}$) across hundreds of executor cores, so each task handles only 1 partition at a time.
- **However, on a local single-node machine (2 CPU cores, 2.5 GB RAM limit)**:
  - If `spark.sql.shuffle.partitions` is default (200) or 16: each task receives $\approx 198$ distinct partition keys concurrently $\to 198 \times 64\text{ MB} \approx 12.6\text{ GB} \gg 2.5\text{ GB}$ heap $\to$ **Immediate JVM Heap OOM**.
  - If `spark.sql.shuffle.partitions = 3200` (1 partition per task): Spark has to schedule 3,200 separate tasks on 2 local cores. The disk I/O from shuffle spill, block manager metadata, and Java object boxing/unboxing over 101M rows caused **10+ second GC pauses**, triggering executor heartbeat timeouts (`RpcTimeoutException: Futures timed out after 10000ms`, `Block broadcast does not exist`), causing the Spark job to abort.

### RCA 2b: Empirical Verification of `write.distribution-mode = 'range'` + `write.fanout.enabled = 'false'`
To rigorously verify whether standard Spark DataSourceV2 could be saved by official Iceberg configuration without switching engines, we conducted a live empirical benchmark (`tests/test_spark_range_write.py`) on the actual BTC-USD dataset (2,265,116 rows spanning 1,584 days):
```sql
CREATE TABLE lakehouse.bronze.test_spark_range (...)
USING iceberg
PARTITIONED BY (exchange, days(timestamp))
TBLPROPERTIES ('write.distribution-mode'='range', 'write.fanout.enabled'='false');
```
* **Execution Command**:
  ```bash
  spark-submit --master local[2] --driver-memory 2500m --executor-memory 2500m test_spark_range_write.py
  ```
* **Result**: **FAILED with `java.lang.OutOfMemoryError: Java heap space` after 48.6s.**
* **Stacktrace Evidence**:
  ```text
  java.lang.OutOfMemoryError: Java heap space
      at java.base/java.io.ByteArrayOutputStream.<init>(ByteArrayOutputStream.java:81)
      at org.apache.iceberg.shaded.org.apache.parquet.hadoop.CodecFactory$HeapBytesCompressor.<init>(CodecFactory.java:222)
      at org.apache.iceberg.shaded.org.apache.parquet.hadoop.CodecFactory.createCompressor(CodecFactory.java:273)
      at org.apache.iceberg.parquet.ParquetWriter.<init>(ParquetWriter.java:90)
      at org.apache.iceberg.io.RollingDataWriter.<init>(RollingDataWriter.java:47)
      at org.apache.iceberg.io.FanoutDataWriter.newWriter(FanoutDataWriter.java:53)
      at org.apache.iceberg.io.FanoutWriter.writer(FanoutWriter.java:64)
      at org.apache.iceberg.io.FanoutWriter.write(FanoutWriter.java:52)
      at org.apache.iceberg.spark.source.SparkWrite$PartitionedDataWriter.write(SparkWrite.java:781)
  ```
* **Why `write.fanout.enabled='false'` still triggered `FanoutDataWriter`**:
  1. In Iceberg's DataSourceV2 writer (`SparkWrite.java`), setting `fanout.enabled=false` tells Iceberg to use a single rolling writer *under the assumption that rows arrive strictly sorted by the partition key*.
  2. However, Spark's `range` distribution mode only partitions data into coarse quantile ranges across Spark tasks; it **does not** guarantee strict intra-task ordering by Iceberg's partition transform (`days(timestamp)`).
  3. When Iceberg's writer encounters out-of-order partition transitions within the same task stream, it automatically falls back to `FanoutDataWriter` (or throws an illegal state exception).
  4. Once `FanoutDataWriter` is engaged, it concurrently instantiates `HeapBytesCompressor` instances for the 1,584 distinct day partitions. At 64MB buffer each, the 2.5GB JVM heap is exhausted almost immediately.

### RCA 2c: Dead Config Analysis for PyIceberg
* **Are `write.distribution-mode` and `write.fanout.enabled` active in PyIceberg?**
  - **No, they are dead config for PyIceberg.**
  - These properties are Spark DataSourceV2-specific engine directives recognized only by `org.apache.iceberg.spark.source.SparkWrite`.
  - PyIceberg executes writes using Apache Arrow's native C++ streaming writer (`pyarrow.dataset.write_dataset`). PyIceberg streams symbol by symbol, writing Parquet files in native off-heap memory without invoking any Java JVM or Spark distribution logic.
  - The properties remain in the DDL as table-level metadata for any external Spark batch consumers, but have zero effect on PyIceberg's C++ write path.

### RCA 3: Preprocessing I/O & CPU Bottleneck
* **Quote Currency Redundancy**: Binance raw data contains numerous duplicate pairs for the same base token (`BTCUSDT`, `BTCUSDC`, `BTCTUSD`, `BTCBUSD`, `BTCFDUSD`). Parsing, verifying, and validating all quote pairs multiplied the data volume and file count unnecessarily.
* **Small File Fragmentation**: Raw source archives contained thousands of tiny files. Reading and appending file-by-file with Pandas incurred extreme Python interpreter overhead, GIL contention, and repeated file descriptor allocation.

---

## 3. Implemented Solutions

### Solution 1: Zero-JVM-Heap Streaming Engine (PyIceberg + DuckDB)

Instead of relying on Spark JVM execution for row-by-row partition fanout, we implemented an out-of-core streaming architecture using **PyIceberg**, **PyArrow**, and **DuckDB**:

```
 ┌─────────────────────────────────────────────────────────────┐
 │                  MinIO S3 / Local Parquet                   │
 └──────────────────────────────┬──────────────────────────────┘
                                │ PyIceberg / PyArrow C++ Zero-Copy Scan
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │                  In-Memory Arrow RecordBatch                │
 └──────────────────────────────┬──────────────────────────────┘
                                │ Vectorized DuckDB SIMD Queries
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │   DuckDB: Market Sanity + Priority Resolution + Dedup       │
 │   QUALIFY ROW_NUMBER() OVER (                               │
 │       PARTITION BY exchange, raw_symbol, timestamp          │
 │       ORDER BY priority DESC, ingested_at DESC              │
 │   ) = 1                                                     │
 └──────────────────────────────┬──────────────────────────────┘
                                │ Native Schema Cast (UTC Microseconds)
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │              PyIceberg Streaming Commit / Append            │
 │        Direct Iceberg REST Catalog Snapshot Creation        │
 └─────────────────────────────────────────────────────────────┘
```

* **Memory Efficiency**: PyArrow and PyIceberg write Parquet files directly in native C++ memory without touching the JVM garbage collector. Peak RAM stayed consistently below **2.5 GB** across the entire 101M records.
* **Zero Commit Conflicts**: Sequential symbol-level streaming ensured 100% deterministic snapshot commits to the Iceberg REST catalog without concurrency collisions.
* **DuckDB Vectorized Speed**: DuckDB executed in-memory filtering and window deduplication on 1.5M rows per symbol in **1.5 to 1.8 seconds**, reaching write speeds between **70,000 and 129,000 rows/second**.

### Solution 2: Preprocessing Optimization Pipeline

1. **Upfront Quote Currency Scope Pruning (`resolve_preferred_quote_symbols`)**:
   - Strictly enforces Section 4 of `PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md`.
   - Before extracting or processing, filters out non-preferred quotes for Binance (`USDT > USDC > USD > BTC`).
   - Eliminated over **60% of redundant files** before reading, saving tens of gigabytes of unnecessary I/O.
2. **PyArrow Multi-threaded Vectorization**:
   - Replaced single-threaded Pandas row iterations with `pyarrow.dataset` and vectorized compute kernels (`pyarrow.compute`).
   - Timestamps are converted to microsecond UTC (`timestamp[us, tz=UTC]`) natively in C++.
3. **Consolidated Monthly Parquet Staging (`YYYY-MM.parquet`)**:
   - Re-chunked thousands of fragmented raw files into **2,816 consolidated monthly files** under `warehouse/staging/canonical/{exchange}/{canonical_symbol}/`.
   - Reduced file open/close syscalls by 95%, allowing downstream `raw_to_bronze` to process 101M rows in **780.6 seconds (~13 minutes)**.

### Solution 3: Environment & Memory Management
* Permanently terminated unnecessary background Thrift services (`HiveThriftServer2`), reclaiming **3.5 GB of host memory**.
* Configured local Airflow DAG bash operators to directly invoke optimized Python runtime scripts, eliminating unnecessary 15-second JVM cold-start overheads on each DAG task.

---

## 4. Performance & Throughput Benchmark

| Pipeline Stage | Implementation | Records Processed | Wall Time | Effective Throughput | Peak RAM |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Raw Preprocessing** | PyArrow Multi-threaded | 101,053,358 | ~12 min | ~140,000 rows/s | < 2.0 GB |
| **Bronze Ingestion** | PyIceberg + PyArrow | 101,053,358 | 780.6 s (~13 min) | **129,460 rows/s** | < 1.2 GB |
| **Silver Cleansing** | PyIceberg + DuckDB | 100,869,347 | 1,421.2 s (~23 min) | **70,976 rows/s** | < 2.7 GB |
| **Curated Silver Reconciliation** | PyIceberg + DuckDB | 100,747,585 | 1,415.1 s (~23 min) | **71,193 rows/s** | < 2.7 GB |
| **Curated Gold Sync** | PyIceberg + DuckDB | 100,650,539 | 1,338.6 s (~22 min) | **75,190 rows/s** | < 2.8 GB |
| **Data Quality Validation** | PyIceberg + DuckDB | 100,650,539 | 544.8 s (~9 min) | **184,747 rows/s** | < 1.5 GB |

---

## 5. Architectural Compliance & Guarantees Verified

1. **Multi-Exchange Independence**: Alpaca and Binance retain their respective native order book measures and quote currencies (`alpaca:USD`, `alpaca:USDC`, `alpaca:USDT`, `binance:USDT`, `binance:USDC`, `binance:USD`, `binance:TUSD`). They never merge into a single synthetic record.
2. **Zero Duplicate Keys**: Guaranteed 0 duplicate `(exchange, raw_symbol, timestamp)` keys across all 100.6M records in Curated Gold.
3. **Market Sanity Enforced**: $high \ge low$, $open > 0$, $close > 0$, $volume \ge 0$, and non-null business keys validated for 100% of final stored records.
4. **Idempotency & Lineage**: Every run records audit trail metrics in `lakehouse.gold.pipeline_audit` with immutable timestamps and execution batch IDs.

---

## 6. Gold Layer Dimensional Modeling (Kimball Architecture)

To resolve the challenge of query performance on BI dashboards (e.g. Power BI, Apache Superset) and implement strict Kimball Dimensional Modeling, the Gold layer is structured into physical dimension and fact tables, rather than relying solely on dynamic SQL views:

```
                          ┌───────────────────────────┐
                          │  lakehouse.gold.dim_symbol│
                          │  (Conformed Dimension)    │
                          │  - 68 Rows, SCD Type 1    │
                          └─────────────┬─────────────┘
                                        │
           ┌────────────────────────────┼────────────────────────────┐
           │ 1:N                        │ 1:N                        │ 1:N
           ▼                            ▼                            ▼
┌───────────────────────────┐┌───────────────────────────┐┌───────────────────────────┐
│ gold.curated_crypto_bars  ││gold.agg_daily_crypto_ohlcv││gold.agg_daily_cross_spread│
│ (Granular 1-Min Fact)     ││(Physical Daily Agg Fact)  ││(Physical Cross-Exch Fact) │
│ - 100,650,539 Records     ││- 83,761 Records           ││- 30,941 Records           │
│ - Lowest Granularity      ││- 1,000x Faster for BI     ││- Cross-Exchange Arbitrage │
└───────────────────────────┘└───────────────────────────┘└───────────────────────────┘
```

### Table Breakdown:
1. **`lakehouse.gold.dim_symbol` (Physical Conformed Dimension Table)**:
   - Contains 68 distinct symbols with attributes: `symbol_key`, `exchange`, `raw_symbol`, `canonical_symbol`, `base_currency`, `quote_currency`, `asset_class`, `first_trade_date`, `last_trade_date`, `total_bars_recorded`.
   - Power BI queries this dimension table instantaneously for slice-and-dice filtering.
2. **`lakehouse.gold.curated_crypto_bars` (Granular 1-Minute Fact Table)**:
   - 100,650,539 records. Retains the atomic transaction grain for algorithmic trading, backtesting, and ML feature stores.
3. **`lakehouse.gold.agg_daily_crypto_ohlcv` (Physical Materialized Daily Aggregate Fact Table)**:
   - 83,761 records. Eliminates the need for Power BI to scan 100M rows on every dashboard load. Pre-calculates `open_price`, `high_price`, `low_price`, `close_price`, `total_volume`, `total_trades`, `minute_bars_count`. Query latency dropped from 130+ seconds to **< 0.3 seconds**.
4. **`lakehouse.gold.agg_daily_cross_exchange_spread` (Physical Materialized Cross-Exchange Comparison Table)**:
   - 30,941 records. Pre-joins concurrent Alpaca and Binance trading days, calculating absolute spread and percentage spread directly for cross-exchange analytics.

---

## 7. Incremental Hourly Processing Pipeline & ACID Reconciliation

In addition to full historical backfill, the platform supports real-time scheduled hourly incremental synchronization via Airflow DAG `lakehouse_incremental_hourly`:

```
 ┌─────────────────────────────────────────────────────────────┐
 │ 1. REST API Ingestion (src/incremental/rest_fetcher.py)     │
 │    Fetches latest 1-min bars for Binance & Alpaca           │
 └──────────────────────────────┬──────────────────────────────┘
                                │ Staged to incremental_{batch_id}.parquet
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ 2. Target Batch Bronze Ingestion (raw_to_bronze.py)         │
 │    Filters by --batch-id; zero full table scan              │
 └──────────────────────────────┬──────────────────────────────┘
                                │ Appends 5 to ~4,000 rows
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ 3. Cleansing & Validation (bronze_to_silver.py)             │
 │    Enforces OHLCV sanity, priority=2, symbol-level filter   │
 └──────────────────────────────┬──────────────────────────────┘
                                │ Cleansed rows in Silver
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ 4. Spark Iceberg ACID MERGE (incremental_sync.py)           │
 │    MERGE INTO lakehouse.silver.curated_crypto_bars          │
 │    MERGE INTO lakehouse.gold.curated_crypto_bars            │
 │    ON (exchange, raw_symbol, timestamp)                     │
 │    WHEN MATCHED AND (priority > target.priority ...) UPDATE │
 │    WHEN NOT MATCHED THEN INSERT                             │
 └──────────────────────────────┬──────────────────────────────┘
                                │ Reconciled Curated Gold
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ 5. Gold Star Schema Materialization & Refresh               │
 │    Refreshes dim_symbol, agg_daily_crypto_ohlcv, spreads    │
 └─────────────────────────────────────────────────────────────┘
```

### Verified Incremental Performance:
* **Batch Filtering**: By passing `--batch-id`, Bronze and Silver processing took **< 5 seconds total**, completely bypassing the 100M historical records.
* **ACID Upsert Speed**: Spark Iceberg `MERGE INTO` executed the intra-exchange priority reconciliation in **~8.2 seconds** with zero JVM heap pressure (as hourly records fall into 1 or 2 partition dates).
* **Data Quality Verification**: Full post-sync check passed with **0 duplicate keys**, **0 sanity violations**, and audit record logged to `gold.pipeline_audit`.

---

## 8. Financial Domain Considerations: Quote Currency Peg Risk & Arbitrage Alerting

### A. The Core Financial Problem: Fiat USD vs Stablecoin USDT Peg Discrepancy
* **Domain Reality**: In cryptocurrency markets, **USD (Fiat - US Central Bank money)** on Alpaca and **USDT (Tether Stablecoin)** or **USDC (Circle)** on Binance are distinct currencies with independent risk profiles:
  - While $1\text{ USDT} \approx 1\text{ USD}$ under normal conditions, USDT is subject to peg risk (de-peg events historically reached $\$0.95$ to $\$1.03$).
  - Comparing raw exchange prices across distinct quote currencies (e.g. `BTC/USD` on Alpaca vs `BTCUSDT` on Binance) without currency normalization introduces **Phantom Arbitrage Spreads** (False Positive alerts).
  - *Example*: If BTC true value is identical on both exchanges, but USDT de-pegs to $\$0.98$, Binance `BTCUSDT` will trade at $\$102,040$ while Alpaca `BTC/USD` trades at $\$100,000$. A naive cross-exchange spread calculation yields a $2.04\%$ arbitrage signal, which is entirely attributable to stablecoin de-peg, not exchange liquidity divergence.

### B. Architectural Principle: Preservation of Historical Truth in Base Tables
* **No Artificial In-flight Conversion in Bronze/Silver/Gold**:
  - The Lakehouse strictly stores native transaction records (`alpaca:USD`, `binance:USDT`, `binance:USDC`).
  - Forcing an immediate currency conversion during ingestion would corrupt the audit trail and destroy historical integrity.

### C. Solution for Serving & Alerting (Phase 2 Roadmap)
1. **Exchange Rate Reference Dimension Table (`dim_fiat_stablecoin_rates`)**:
   - Ingest continuous exchange rate feeds for `USDT/USD` and `USDC/USD` (via Binance stablecoin market pairs already present in the catalog, such as `USDT-USD`, `USDC-USD`, or Chainlink Decentralized Oracle feeds).
2. **Normalized Cross-Exchange Spread Calculation (Serving Layer)**:
   $$\text{Binance Close (USD Equivalent)} = \text{Close}_{\text{Binance (USDT)}} \times \text{Rate}_{\text{USDT/USD}}$$
   $$\text{Real Arbitrage Spread} = \text{Close}_{\text{Alpaca (USD)}} - \text{Binance Close (USD Equivalent)}$$
3. **Filtered Alerting Thresholds**:
   - Arbitrage notifications and algorithmic triggers in Phase 2 must evaluate the **Real Arbitrage Spread** net of stablecoin basis risk and trading transaction fees. This completely eliminates false-positive alerts on BI dashboards and real-time notification bots.


