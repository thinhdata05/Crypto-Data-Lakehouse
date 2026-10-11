-- ==============================================================================
-- DDL: Apache Iceberg Tables for Multi-Exchange Crypto Lakehouse
-- In accordance with PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md
-- Business Key: (exchange, raw_symbol, timestamp)
-- Catalog: lakehouse
-- ==============================================================================

-- Create Namespaces
CREATE DATABASE IF NOT EXISTS lakehouse.bronze;
CREATE DATABASE IF NOT EXISTS lakehouse.silver;
CREATE DATABASE IF NOT EXISTS lakehouse.gold;

-- 1. Bronze Table: Raw canonical ingestion partitioned by (exchange, days(timestamp))
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
PARTITIONED BY (exchange, days(timestamp))
TBLPROPERTIES ('write.distribution-mode'='range', 'write.fanout.enabled'='false');

-- 2. Silver Table (Per-Exchange Cleansed): Partitioned by (exchange, days(timestamp))
CREATE TABLE IF NOT EXISTS lakehouse.silver.crypto_bars (
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
PARTITIONED BY (exchange, days(timestamp))
TBLPROPERTIES ('write.distribution-mode'='range', 'write.fanout.enabled'='false');

-- 3. Curated Silver Table: Reconciled per-exchange (MERGE on exchange, raw_symbol, timestamp)
CREATE TABLE IF NOT EXISTS lakehouse.silver.curated_crypto_bars (
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
PARTITIONED BY (exchange, days(timestamp))
TBLPROPERTIES ('write.distribution-mode'='range', 'write.fanout.enabled'='false');

-- 4. Curated Gold Table: Dimensional model for reporting, analytics, and ML training
CREATE TABLE IF NOT EXISTS lakehouse.gold.curated_crypto_bars (
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
    last_updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(timestamp), exchange)
TBLPROPERTIES ('write.distribution-mode'='range', 'write.fanout.enabled'='false');

-- 5. Conformed Dimension: dim_symbol (SCD Type 1)
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_symbol (
    symbol_key BIGINT,
    exchange STRING,
    raw_symbol STRING,
    canonical_symbol STRING,
    base_currency STRING,
    quote_currency STRING,
    asset_class STRING,
    first_trade_date DATE,
    last_trade_date DATE,
    total_bars_recorded BIGINT,
    created_at TIMESTAMP
)
USING iceberg;

-- 6. Materialized Daily Aggregate Fact Table: agg_daily_crypto_ohlcv
CREATE TABLE IF NOT EXISTS lakehouse.gold.agg_daily_crypto_ohlcv (
    exchange STRING,
    canonical_symbol STRING,
    base_currency STRING,
    quote_currency STRING,
    trade_date DATE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    total_volume DOUBLE,
    total_trades BIGINT,
    minute_bars_count BIGINT,
    last_updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (exchange, days(trade_date));

-- 7. Materialized Daily Cross-Exchange Comparison Table: agg_daily_cross_exchange_spread
CREATE TABLE IF NOT EXISTS lakehouse.gold.agg_daily_cross_exchange_spread (
    base_currency STRING,
    trade_date DATE,
    alpaca_close DOUBLE,
    alpaca_volume DOUBLE,
    binance_close DOUBLE,
    binance_volume DOUBLE,
    spread_abs DOUBLE,
    spread_pct DOUBLE,
    last_updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(trade_date));

-- 8. Pipeline Audit Table: Lineage and Data Quality tracking
CREATE TABLE IF NOT EXISTS lakehouse.gold.pipeline_audit (
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

