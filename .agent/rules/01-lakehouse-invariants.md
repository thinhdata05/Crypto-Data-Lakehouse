---
description: Core architectural invariants and storage rules for Apache Iceberg Medallion tables
globs: "src/pipeline/**, sql/**, airflow/**, src/preprocessing/**"
---

# 01. Lakehouse Core Invariants & Storage Rules

Mandatory architectural invariants for all development, optimization, and data pipeline modifications across the Lakehouse.

---

## 1. Primary Business Key & Unique Identity
- The sole composite **Business Key** across all Lakehouse layers is:
  $$\text{Business Key} = (\mathbf{exchange}, \mathbf{raw\_symbol}, \mathbf{timestamp})$$
- **MANDATORY RULE**:
  - **NEVER** use `canonical_symbol` or `(canonical_symbol, timestamp)` as a primary storage key.
  - Rationale: An asset exist independently across Alpaca (`BTC_USD`) and Binance (`BTCUSDT`). Retaining `(exchange, raw_symbol, timestamp)` prevents cross-exchange collisions and preserves native market identity.

---

## 2. Multi-Exchange Independence
- **Alpaca** and **Binance** operate independent order books, liquidity pools, and matching engines.
- Records for the same 1-minute timestamp on Alpaca and Binance:
  - Are **NEVER** duplicates.
  - Must **NEVER** overwrite or deduplicate against each other.
  - Both records must coexist in `silver.bars` and `gold.*` to support cross-exchange spread and arbitrage analytics.

---

## 3. Intra-Exchange Priority Conflict Resolution ($3 > 2 > 1$)
- The platform enforces 3 priority tiers (`priority`, defined in `src/preprocessing/normalizer.py`):
  - **Priority 3 (`UPDATED_REALTIME_PRIORITY = 3`, Highest)**: Late exchange correction over WebSocket (`T = 'u'`) or audited manual override (`source = 'correction'`).
  - **Priority 2 (`HISTORICAL_PRIORITY = 2`, Medium)**: Settled REST API bars — covering both **Historical Backfill** and **Hourly Incremental REST API** (`source = 'historical'`). Ties within Priority 2 are broken by `ingested_at DESC`.
  - **Priority 1 (`INITIAL_REALTIME_PRIORITY = 1`, Lowest)**: Preliminary real-time WebSocket bar (`T = 'b'`, `source = 'realtime'`).
- **GOLDEN RULE**:
  - Priority resolution ($3 > 2 > 1$, tie-breaker `ingested_at DESC`) applies **STRICTLY WITHIN THE SAME EXCHANGE (`exchange`)**.
  - A `priority = 2` (REST) or `priority = 3` (Correction) bar from Alpaca overwrites a `priority = 1` bar from Alpaca in `silver.bars`, but **never** affects Binance records.

---

## 4. Iceberg Table Partitioning Specification
- All 1-minute bar tables (`bronze.crypto_bars`, `bronze.rt_bars`, `silver.bars`) must use daily time partitioning:
  $$\text{PARTITIONED BY (days(timestamp))}$$
- Operational Control Plane tables (`ops.ingestion_manifest`, `ops.pipeline_audit`) are partitioned by `days(processed_at)` and `days(created_at)`.
- **Strictly Forbidden Anti-Patterns**:
  - **FORBIDDEN**: Partitioning by `exchange`, `raw_symbol`, or `canonical_symbol` (causes small-file explosion across 68 symbols $\times$ 1,800+ days).
  - **FORBIDDEN**: Unconstrained sub-day partitioning (hours/minutes).

---

## 5. Deduplication & Scoped Partition Overwrite / MERGE Standard
- When deduplicating in `silver.bars` (via DuckDB or Spark):
  ```sql
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY exchange, raw_symbol, timestamp
      ORDER BY priority DESC, ingested_at DESC
  ) = 1
  ```
- When reconciling an incremental window into `lakehouse.silver.bars` (`src/pipeline/incremental/incremental_sync.py`):
  - Expand `[min_ts, max_ts]` to UTC day boundaries `[day_start_iso, day_end_iso)` via `compute_partition_day_bounds()`.
  - Scan existing `silver.bars` strictly within that bounded daily partition window, reconcile in-memory via DuckDB (`3 > 2 > 1`, tie-break `ingested_at DESC`), and atomically overwrite only the affected daily partitions (`silver_tbl.overwrite(reconciled_arr, overwrite_filter=partition_window_filter)`).

---

## 6. Control Plane (`ops.*`) vs. Data Plane (`bronze.*`, `silver.*`, `gold.*`)
1. **Control Plane (`lakehouse.ops.*`) — Isolated from Medallion Business Layers**:
   - **`lakehouse.ops.ingestion_manifest`**: File-level SHA-256 lineage, record counts, min/max timestamps, and ingestion checkpoints (`PARTITIONED BY (days(processed_at))`).
   - **`lakehouse.ops.pipeline_audit`**: Execution telemetry, duration, row counts, and quality check outcomes across all pipeline steps (`PARTITIONED BY (days(created_at))`).
2. **Bronze Layer (`lakehouse.bronze.crypto_bars` on Local / `lakehouse.bronze.rt_bars` on Cloud)**:
   - **Canonicalized Raw Ingestion Layer (Append-Only)**: Stores standardized canonical bars (pruned to primary quote pairs, timestamp >= 2021-01-01, normalized types) with `ingested_at` and `batch_id` without in-place mutation. True external raw dumps remain preserved indefinitely in `./raw/`.
3. **Silver Layer (`lakehouse.silver.bars` — Unified Authoritative Silver Table on Local & Cloud)**:
   - Single authoritative Silver table (no split between `silver.crypto_bars` and `silver.curated_crypto_bars`).
   - Enforces in a single pass: 7-rule OHLCV mathematical sanity filtering + non-negative trade count, dynamic priority assignment ($3 > 2 > 1$), and strict deduplication on `(exchange, raw_symbol, timestamp)` ordered by `priority DESC, ingested_at DESC`.
4. **Gold Layer (`lakehouse.gold.*` — Exclusively on Cloud)**:
   - Conformed dimensions (`dim_symbol`, `dim_date`), daily pre-computed aggregates (`agg_daily_crypto_ohlcv`, `agg_daily_cross_exchange_spread`), and 7 semantic serving views (specified in `docs/3-specifications/phase2-cloud-setup-spec.md`). Local never builds Gold tables.

---

## 7. Zero-JVM-OOM & Partition-Aligned Coalescing Architecture
- Never use Spark `PartitionedFanoutWriter` for multi-year historical ingestion across 1,800+ daily partitions (causes JVM Heap OOM).
- Always use **PyIceberg + DuckDB in-process streaming** with **Partition-Aligned Window Batching**:
  - `raw_to_bronze.py`: Groups all 68 symbols by **Quarter (`YYYY-Q1..Q4`)**, producing **1 coalesced Parquet file per UTC day per batch window** (`2,071` files total instead of `84,149` small files). Across incremental micro-batches, multiple data files accumulate until scheduled Iceberg table compaction is triggered.
  - `bronze_to_silver.py`: Streams `bronze.crypto_bars` by **Calendar Month (`YYYY-MM`)** across all 68 symbols (`Peak RAM < 800 MB`, `2,058` coalesced daily Parquet files). Throughput figures (~986k rows/s) and RAM bounds reflect empirical test benchmarks on local developer workstations.
  - Always call `pa.default_memory_pool().release_unused()` after each window.

---

## 8. Phase 2 Hybrid Architecture: Single-Writer Convergence at Cloud Silver (Option 1)
- **Local Medallion (`s3://lakehouse/` — Lightweight Producer)**:
  - Processes `raw/` $\to$ `staging/canonical/` $\to$ `bronze.crypto_bars` $\to$ `silver.bars`.
  - Historical `silver.bars` (`104,657,836` rows, `priority = 2`) is **planned to be cold bootstrapped once** to Cloud `silver.bars` upon Phase 2 launch.
  - Hourly incremental DAG (`lakehouse_incremental_hourly`) reconciles REST bars into Local `silver.bars` and publishes immutable per-exchange Parquet files + atomic `_manifest.json` to `confirmed/` (with verified S3 upload when configured). Local **never** builds `gold.*` (preventing Split-Brain).
- **Proposed Cloud Medallion (`s3://<cloud>-lakehouse/` — Target Architecture Specification)**:
  - Phase 2 Cloud architecture is fully specified in `docs/3-specifications/phase2-cloud-setup-spec.md` as independent AWS Managed Services (not implemented in this repository).
  - **Planned Dual WebSocket Ingestion**: Designed to stream `WS Alpaca + WS Binance` via ECS Fargate / MSK into Cloud `bronze.rt_bars` (`priority = 1` for `T='b'`, `priority = 3` for `T='u'`).
  - **Planned Single Writer (AWS EMR Serverless / Glue / ECS Job)**: Designed to merge Cloud `bronze.rt_bars` + `confirmed/` Parquets into Cloud `silver.bars` (`3 > 2 > 1`, `ingested_at DESC`) and refresh **`gold.*`** on Cloud.
