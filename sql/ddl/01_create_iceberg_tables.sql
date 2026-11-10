-- ==============================================================================
-- DDL: Local Producer Apache Iceberg Tables (Textbook Senior DE Architecture)
-- Business Key: (exchange, raw_symbol, timestamp)
-- Catalog: lakehouse (Local MinIO s3://lakehouse/)
-- Architecture:
--   1. Control Plane (`lakehouse.ops`):
--      - `ops.ingestion_manifest`: File-level SHA-256 checkpointing & lineage.
--      - `ops.pipeline_audit`: Operational DAG execution & DQ audit log.
--   2. Data Plane (`lakehouse.bronze` & `lakehouse.silver`):
--      - `bronze.crypto_bars`: Immutable append-only raw canonical ingestion.
--      - `silver.bars`: Cleansed, OHLCV-validated, deduplicated (3 > 2 > 1)
--        authoritative Local table that publishes immutable Parquet + `_manifest.json`
--        to `confirmed/` for Cloud convergence.
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS lakehouse.ops;
CREATE DATABASE IF NOT EXISTS lakehouse.bronze;
CREATE DATABASE IF NOT EXISTS lakehouse.silver;

-- 1. Control Plane: Ingestion Manifest Table (Checkpointing & Idempotency Gate)
CREATE TABLE IF NOT EXISTS lakehouse.ops.ingestion_manifest (
    source_file STRING,
    exchange STRING,
    raw_symbol STRING,
    batch_id STRING,
    file_hash STRING,
    record_count BIGINT,
    min_timestamp TIMESTAMP,
    max_timestamp TIMESTAMP,
    status STRING,
    processed_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(processed_at));

-- 2. Control Plane: Pipeline Audit Table (Operational Observability & DQ Tracking)
CREATE TABLE IF NOT EXISTS lakehouse.ops.pipeline_audit (
    run_id STRING,
    dag_id STRING,
    step_name STRING,
    status STRING,
    records_processed BIGINT,
    details STRING,
    created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at));

-- 3. Bronze Layer: Immutable Append-Only Raw Canonical Bars
CREATE TABLE IF NOT EXISTS lakehouse.bronze.crypto_bars (
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
PARTITIONED BY (days(timestamp))
TBLPROPERTIES ('write.distribution-mode'='range', 'write.fanout.enabled'='false');

-- 4. Silver Layer: Authoritative Cleansed & Deduplicated Bars (3 > 2 > 1 Priority)
CREATE TABLE IF NOT EXISTS lakehouse.silver.bars (
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
    priority INT,
    base_currency STRING,
    quote_currency STRING,
    canonical_symbol STRING,
    batch_id STRING,
    ingested_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(timestamp))
TBLPROPERTIES ('write.distribution-mode'='range', 'write.fanout.enabled'='false');
