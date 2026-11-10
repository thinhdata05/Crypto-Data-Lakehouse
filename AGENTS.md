# AGENTS.md: AI Agent Operational Instructions & Lakehouse Guidelines

Welcome, AI Agent, to the **Multi-Exchange Crypto Lakehouse Platform**.
Before executing any action, you **MUST** read and strictly adhere to the following operational rules and project state documents:

---

## 1. Quick Navigation & Context
- [**`MEMORY.md`**](file:///e:/project_hybrid_lakehouse_crypto/MEMORY.md): Authoritative record of the current Lakehouse state (`104,706,818` Bronze records, `104,657,836` Silver records across 68 symbols), audit results, and Phase 2 Hybrid Cloud Roadmap.
- [**`README.md`**](file:///e:/project_hybrid_lakehouse_crypto/README.md): Human-facing Vietnamese documentation covering Medallion architecture, data flows, table schemas, and quickstart commands.

---

## 2. Agent Operational Rules Index (`.agent/rules/`)
All core operational invariants are defined in [`.agent/rules/`](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/):

1. [**`01-lakehouse-invariants.md`**](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/01-lakehouse-invariants.md):
   - **Business Key**: `(exchange, raw_symbol, timestamp)`.
   - **Multi-Exchange Independence**: Never merge or overwrite records across Alpaca and Binance.
   - **Intra-Exchange Priority**: Priority resolution $3 > 2 > 1$ (`3`: WebSocket late correction `T='u'`, `2`: Historical Backfill & Incremental REST API `source='historical'`, `1`: Initial WebSocket `T='b'`, tie-broken by `ingested_at DESC`) applies **strictly within the same exchange**.
   - **Partitioning**: Strictly `PARTITIONED BY (days(timestamp))` for all minute-bar tables. Partitioning by `exchange` or `raw_symbol` is strictly forbidden.
   - **Namespace Separation (Control Plane vs. Data Plane)**:
     - **Control Plane (`lakehouse.ops`)**: `ops.ingestion_manifest` (`days(processed_at)`) and `ops.pipeline_audit` (`days(created_at)`).
     - **Local Data Plane (`s3://lakehouse/`)**: `bronze.crypto_bars` (immutable append-only) and unified `silver.bars` (7-rule OHLCV validated, dynamic priority $3 > 2 > 1$, deduplicated).
   - **Phase 2 Hybrid Convergence (Option 1 — Local Lightweight Producer)**:
     - Local (`s3://lakehouse/`) stops at **`silver.bars`** (no Local `gold.*` tables, preventing Split-Brain) and publishes immutable Parquet slices + `_manifest.json` to `confirmed/`.
     - Cloud (`s3://<cloud>-lakehouse/`) ingests dual WebSocket streams (`WS Alpaca + WS Binance`) into `bronze.rt_bars`, converges with `confirmed/` via a Single Writer at `silver.bars`, and maintains the **Single Source of Truth for `gold.*`**.

2. [**`02-financial-domain-rules.md`**](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/02-financial-domain-rules.md):
   - **Quote Currency Preservation**: Never convert `USDT` or `USDC` to `USD` inside base physical tables (`bronze.*`, `silver.bars`).
   - **Quote Hierarchy**: Select one primary liquidity pair per asset per exchange (`Binance: USDT > USDC > USD > BTC`, `Alpaca: USD > USDT > USDC`).
   - **Normalized Spread Serving**: Perform FX/stablecoin normalization exclusively in serving views (`v_daily_cross_exchange_normalized_spread` via `v_fiat_stablecoin_rates`) or bounded API (`src/serving/query_minute_spread.py`).
   - **7-Rule OHLCV Sanity**: Enforce all 7 mathematical inequalities (`high >= low`, `high >= open`, `high >= close`, `low <= open`, `low <= close`, `open/high/low/close > 0`, `volume >= 0`).
   - **No Synthetic Rows**: Start strictly at the real exchange listing timestamp ($\ge 2021-01-01$ UTC); never forward-fill or fabricate missing bars.

3. [**`03-safety-and-devops.md`**](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/03-safety-and-devops.md):
   - **Containerized Execution**: Run Spark SQL, PyIceberg pipelines, and Airflow CLI inside Docker containers on `lakehouse-net`.
   - **Zero-JVM-OOM & Partition Coalescing**: Use PyIceberg + DuckDB in-process streaming with partition-aligned window batching (Quarter windows for Bronze, Month windows for Silver) to prevent small-file explosion and VHDX bloat.
   - **Partition Pushdown Protection**: Always include `WHERE timestamp >= ... AND timestamp < ...` when querying 100M+ rows.
   - **Storage & Filesystem Semantics**:
     - `./staging` is a transient local staging buffer (`staging/canonical` and `staging/confirmed`).
     - MinIO `s3://lakehouse/` is the true physical Iceberg Lakehouse storage.
   - **Idempotency & Secrets**: All pipelines must be idempotent on replay; read secrets exclusively from `.env`.

4. [**`04-documentation-rules.md`**](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/04-documentation-rules.md):
   - **Language Convention**:
     - **Agent files** (`AGENTS.md`, `MEMORY.md`, `.agent/rules/*.md`): Written in **English** for optimal AI Agent comprehension.
     - **Human documentation** (`README.md`, `docs/**/*.md`): Written in **Vietnamese** for the user.
   - **Auto-Documentation Reflex**: Automatically update `MEMORY.md`, `README.md`, and `docs/` whenever schemas, pipelines, or benchmarks change.
   - **Bug & Optimization Logging**: Record every RCA and Before/After benchmark (Peak RAM, Rows/s, Wall-clock time, VHDX impact) in `docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md`.
   - **Single Source of Truth for Cloud Views**: Reference all 7 Gold analytical views in `docs/3-specifications/phase2-cloud-setup-spec.md`.
