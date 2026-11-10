# PROJECT MEMORY: Multi-Exchange Crypto Lakehouse Platform

## 1. Project Identity & Current Verified Status
- **Platform**: Multi-Exchange Crypto Lakehouse Platform (Apache Iceberg, PyIceberg, DuckDB, Spark SQL, MinIO, Airflow)
- **Current Phase**: **Phase 1 & Phase 2 Local Producer Complete (Textbook Senior DE Architecture)**
- **Physical Storage Bucket**: `s3://lakehouse/` (MinIO S3-compatible object storage)
- **Verified Local Lakehouse Metrics (`run_id = 28c4df2d-6b52-4c3e-9721-065a0fdd5f79` & Incremental DAG `056d4b65-cc90-4b89-99fb-166f24d3dd22`)**:
  - `lakehouse.bronze.crypto_bars`: **`104,706,818` historical bars** (+ incremental REST bars) across **`2,071`** coalesced daily Parquet files (`0` preprocessing errors; permissive schema promotion recovered 123 Alpaca months / `3,653,460` rows).
  - `lakehouse.silver.bars`: **`104,657,836` historical bars** (+ incremental reconciled bars) across **`2,058`** coalesced daily Parquet files (`48,982` invalid/duplicate bars filtered out; `0` duplicate keys, `0` OHLCV sanity violations).
  - `lakehouse.ops.ingestion_manifest`: **`3,007` SHA-256 lineage entries** (`2,939` monthly file hashes + `68` symbol-level batch summaries + incremental file hashes).
  - `lakehouse.ops.pipeline_audit`: Execution telemetry across `raw_to_bronze`, `bronze_to_silver`, `incremental_sync`, and `data_validation`.
- **Covered Universe**: **68 market symbols** (**34 Alpaca** in `USD`/`USDT`/`USDC` + **34 Binance** in `USDT`/`USDC`/`USD`/`TUSD`).
- **Automated Test & DAG Verification**:
  - **25/25 Unit & Live Integration Tests**: `PASS` in `~3.0s` (`python3 -m unittest discover -s tests -v`), covering partition invariants, multi-exchange independence, OHLCV sanity (including trade_count >= 0), atomic manifest generation, S3 upload verification, and live single-writer convergence.
  - **Airflow Incremental Hourly DAG (`lakehouse_incremental_hourly`)**: `SUCCESS` end-to-end across all tasks in `~34s` (`rest_fetcher` -> `raw_to_bronze` -> `incremental_sync` -> `publish_confirmed_parquet` -> `data_validation`). Local stops cleanly at handoff.
- **Phase 2 Cloud Architectural Decoupling**:
  - The Local repository is strictly a **Lightweight Producer** (Local Data Plane stops at `lakehouse.silver.bars` $\to$ publishes confirmed Parquet slices + `_manifest.json` to handoff directory/S3 bucket).
  - All Phase 2 Cloud components (Dual-Exchange Realtime WebSocket Ingestion, Cloud Single-Writer Convergence Worker, Cloud Iceberg `silver.bars`, and `lakehouse.gold.*`) are decoupled external AWS services specified in `docs/3-specifications/phase2-cloud-setup-spec.md`. The Local codebase contains zero Cloud execution code or Gold tables.
- **Authoritative Gold Views**:
  - Exactly **7 views** specified in `docs/3-specifications/phase2-cloud-setup-spec.md` (`v_daily_crypto_ohlcv`, `v_hourly_crypto_ohlcv`, `v_cross_exchange_daily_spread`, `v_cross_exchange_comparison`, `v_pipeline_audit_summary`, `v_fiat_stablecoin_rates`, `v_daily_cross_exchange_normalized_spread`). Local maintains zero Gold tables or views.
- **Benchmark Realism**:
  - Historical backfill throughput (~986k rows/s) and RAM usage (< 800 MB) are empirical single-run benchmarks on 104M+ rows using DuckDB streaming windows on local developer hardware, not guaranteed platform SLAs.
  - File coalescing achieves 1 file per partition day per batch window; continuous incremental micro-batches produce separate files until Iceberg table compaction is executed.
- **Infrastructure & Disk Footprint**:
  - All 6 Docker containers (`lakehouse-minio`, `lakehouse-iceberg-rest`, `lakehouse-spark-iceberg`, `lakehouse-postgres`, `lakehouse-airflow-webserver`, `lakehouse-airflow-scheduler`) healthy on `lakehouse-net`.
  - WSL2 virtual disk `E:\DockerWSL\docker_data.vhdx` compacted via `fstrim -a -v` + `diskpart compact vdisk` from `86.35 GB` down to **`39.32 GB`** (`47.34 GB` free space on drive `E:`).

---

## 2. Documentation Index & Language Convention
- **Agent Operational Files (Written in English)**:
  - [`AGENTS.md`](file:///e:/project_hybrid_lakehouse_crypto/AGENTS.md): Entry-point instructions for AI Agents.
  - [`MEMORY.md`](file:///e:/project_hybrid_lakehouse_crypto/MEMORY.md): Authoritative system state, metrics, and architectural decisions.
  - [`.agent/rules/01-lakehouse-invariants.md`](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/01-lakehouse-invariants.md): Core invariants, business key, priority resolution, partitioning, and Hybrid Cloud Option 1.
  - [`.agent/rules/02-financial-domain-rules.md`](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/02-financial-domain-rules.md): Quote currency preservation, stablecoin peg normalization, and 7-rule OHLCV sanity.
  - [`.agent/rules/03-safety-and-devops.md`](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/03-safety-and-devops.md): Container execution, partition pushdown, Python 3.10 timestamp rules, and VHDX disk hygiene.
  - [`.agent/rules/04-documentation-rules.md`](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/04-documentation-rules.md): Auto-documentation reflex and language rules.
- **Human Documentation Files (Written in Vietnamese)**:
  - [`README.md`](file:///e:/project_hybrid_lakehouse_crypto/README.md): Project overview, architecture, pipeline flows, and quickstart commands.
  - [`docs/1-architecture/storage-and-warehouse.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/1-architecture/storage-and-warehouse.md): Deep dive into `s3://lakehouse/`, Control Plane (`ops.*`) vs. Data Plane (`bronze.*`, `silver.bars`, `gold.*`), and Phase 2 Hybrid Cloud Option 1.
  - [`docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md): Post-mortems (Spark JVM OOM, 84k small files & VHDX bloat, Alpaca permissive schema promotion, Python 3.10 ISO-8601 `Z` handling) and quantitative benchmarks.
  - [`docs/3-specifications/phase1-migration-spec.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/3-specifications/phase1-migration-spec.md): Complete technical specification of the Local Medallion Producer (`ops`, `bronze`, `silver.bars`).
  - [`docs/3-specifications/phase2-cloud-setup-spec.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/3-specifications/phase2-cloud-setup-spec.md): Cloud Single-Writer Convergence specification (`bronze.rt_bars`, `silver.bars`, `gold.*`, Dual WebSocket Alpaca + Binance).
  - [`docs/4-runbooks/catalog-partition-migration.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/4-runbooks/catalog-partition-migration.md): Runbook for automated Iceberg partition spec verification and evolution.
  - [`docs/4-runbooks/operations-and-maintenance.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/4-runbooks/operations-and-maintenance.md): Runbook for Docker operations, CLI backfill vs. Airflow incremental scheduling, snapshot expiration, and VHDX compaction.
  - [`docs/5-data-contracts/symbol-and-currency-contract.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/5-data-contracts/symbol-and-currency-contract.md): Data contracts, 68-symbol universe, Hasbrouck Information Share & Granger causality proofs, and `confirmed/` manifest contract.

---

## 3. Core Architectural Invariants (DO NOT VIOLATE)
1. **Business Key**: `(exchange, raw_symbol, timestamp)`.
   - Alpaca `raw_symbol` uses underscores (`BTC_USD`, `ETH_USD`); Binance uses concatenated pairs (`BTCUSDT`, `ETHUSDT`).
2. **Multi-Exchange Independence**:
   - Alpaca and Binance bars at the same timestamp are independent market observations and must never overwrite each other.
3. **Intra-Exchange Priority Resolution ($3 > 2 > 1$)**:
   - `3`: WebSocket late correction (`T='u'`, `source='correction'`).
   - `2`: Historical Backfill & Hourly Incremental REST API (`source='historical'`), tie-broken by `ingested_at DESC`.
   - `1`: Preliminary WebSocket bar (`T='b'`, `source='realtime'`).
   - Enforced **strictly within the same `exchange`**.
4. **Control Plane vs. Data Plane Separation**:
   - `lakehouse.ops.ingestion_manifest` (`PARTITIONED BY (days(processed_at))`): Per-file SHA-256 lineage and idempotency checkpointing.
   - `lakehouse.ops.pipeline_audit` (`PARTITIONED BY (days(created_at))`): Pipeline execution telemetry.
   - `lakehouse.bronze.crypto_bars` (`PARTITIONED BY (days(timestamp))`): Immutable append-only canonical bars on Local.
   - `lakehouse.silver.bars` (`PARTITIONED BY (days(timestamp))`): Single authoritative Silver table on both Local and Cloud (7-rule OHLCV sanity, dynamic priority $3 > 2 > 1$, deduplicated).
5. **Phase 2 Hybrid Architecture (Option 1 — Local Lightweight Producer)**:
   - Local stops at `lakehouse.silver.bars` (no Local `gold.*` tables, eliminating Split-Brain risk) and exports hourly immutable Parquet slices + `_manifest.json` to `confirmed/`.
   - **Proposed Cloud Architecture (Target Design)**: Cloud will ingest Dual WebSocket streams (`WS Alpaca + WS Binance`) into `lakehouse.bronze.rt_bars`, merge `bronze.rt_bars` + `confirmed/` via a Single Writer into Cloud `lakehouse.silver.bars`, and materialize the sole **`lakehouse.gold.*`** layer (`dim_symbol`, `dim_date`, `agg_daily_crypto_ohlcv`, `agg_daily_cross_exchange_spread`, and views specified in `docs/3-specifications/phase2-cloud-setup-spec.md`).

---

## 4. End-to-End Pipeline Flow & Design Rationale
1. **Stage 1 — Pre-Bronze Coalescing & Schema Standardization (`src/preprocessing/alpaca.py`, `src/preprocessing/binance.py`)**:
   - Reads `87,693` raw daily Parquet files (`./raw/alpaca/bars` and `./raw/binance/bars`).
   - Prunes non-primary quote currencies (`Binance: USDT > USDC > USD > BTC`, `Alpaca: USD > USDT > USDC`) to retain 68 canonical symbols.
   - Resolves `int64` vs. `double` `volume` schema drift across daily files via `pa.concat_tables(tables, promote_options="permissive")` and coalesces daily files into `2,939` monthly Parquet files in `./staging/canonical/`.
2. **Stage 2 — Partition-Aligned Bronze Ingestion (`src/pipeline/bronze/raw_to_bronze.py`)**:
   - Verifies per-file SHA-256 hashes against `lakehouse.ops.ingestion_manifest` to skip already-ingested files.
   - Batches all 68 symbols by **Quarter (`YYYY-Q1..Q4`)** before appending to `lakehouse.bronze.crypto_bars` (`s3://lakehouse/bronze/crypto_bars`), coalescing writes into **`2,071` daily Parquet files** (`104,706,818` rows) instead of `84,149` small files.
3. **Stage 3 — Partition-Aligned Silver Transformation (`src/pipeline/silver/bronze_to_silver.py`)**:
   - Streams `lakehouse.bronze.crypto_bars` by **Calendar Month (`YYYY-MM`)** across all 68 symbols (`Peak RAM < 800 MB`, `986,372` rows/s).
   - Applies in-process DuckDB vectorization to filter out 7-rule OHLCV violations, assign dynamic `priority` ($3 > 2 > 1$), and deduplicate on `(exchange, raw_symbol, timestamp)` ordered by `priority DESC, ingested_at DESC`.
   - Writes **`104,657,836` rows** (`48,982` invalid/duplicate bars removed) into **`2,058` daily Parquet files** in `lakehouse.silver.bars`.
4. **Stage 4 — Hourly Incremental Producer (`airflow/dags/lakehouse_incremental_hourly_dag.py`)**:
   - `rest_fetcher.py` $\to$ `raw_to_bronze.py` $\to$ `incremental_sync.py` (scoped daily partition overwrite in `silver.bars`) $\to$ `publish_confirmed_parquet.py` (writes per-exchange Parquet + `_manifest.json` to `staging/confirmed/`) $\to$ `data_validation.py`.
5. **Stage 5 — Partition-Aligned Quality Audit (`src/pipeline/quality/data_validation.py`)**:
   - Validates `silver.bars` in quarterly windows with column projection (`Peak RAM < 500 MB`, `22.7s` runtime) and logs `SUCCESS` to `lakehouse.ops.pipeline_audit`.
