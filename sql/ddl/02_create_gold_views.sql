-- ==============================================================================
-- Gold Analytics & Reporting Views
-- Catalog: lakehouse
-- In accordance with PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md
-- ==============================================================================

-- 1. Daily Aggregated OHLCV Serving View (Backed by Materialized Fact Table)
CREATE OR REPLACE VIEW lakehouse.gold.v_daily_crypto_ohlcv AS
SELECT
    exchange,
    canonical_symbol,
    base_currency,
    quote_currency,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    total_volume,
    total_trades,
    minute_bars_count,
    last_updated_at
FROM lakehouse.gold.agg_daily_crypto_ohlcv;

-- 2. Hourly Aggregated OHLCV Rollup per Exchange
CREATE OR REPLACE VIEW lakehouse.gold.v_hourly_crypto_ohlcv AS
SELECT
    exchange,
    canonical_symbol,
    base_currency,
    quote_currency,
    date_trunc('hour', timestamp) AS trade_hour,
    min_by(open, timestamp) AS open_price,
    max(high) AS high_price,
    min(low) AS low_price,
    max_by(close, timestamp) AS close_price,
    sum(volume) AS total_volume,
    sum(trade_count) AS total_trades,
    count(*) AS minute_bars_count
FROM lakehouse.gold.curated_crypto_bars
GROUP BY exchange, canonical_symbol, base_currency, quote_currency, date_trunc('hour', timestamp);

-- 3. Daily Cross-Exchange Comparison View (Backed by Materialized Table - Instant Response)
CREATE OR REPLACE VIEW lakehouse.gold.v_cross_exchange_daily_spread AS
SELECT
    base_currency,
    trade_date,
    alpaca_close,
    alpaca_volume,
    binance_close,
    binance_volume,
    spread_abs,
    spread_pct,
    last_updated_at
FROM lakehouse.gold.agg_daily_cross_exchange_spread;

-- Backwards compatibility alias for daily cross exchange
CREATE OR REPLACE VIEW lakehouse.gold.v_cross_exchange_comparison AS
SELECT * FROM lakehouse.gold.v_cross_exchange_daily_spread;

-- NOTE ON MINUTE-LEVEL ARBITRAGE / CROSS-EXCHANGE VIEWS:
-- Unguarded minute-level views (e.g. self-joining 100M+ rows on curated_crypto_bars)
-- are deliberately EXCLUDED from public DDL views to prevent accidental Cartesian scans
-- and JVM Heap exhaustion (violating Lakehouse Invariant 01 & Rule 03).
-- To query minute-level cross-exchange spreads safely, use the bounded serving API:
-- python src/serving/query_minute_spread.py --base-currency BTC --start-time <ISO> --end-time <ISO>
-- which strictly enforces partition pruning and max window constraints.

-- 4. Pipeline Audit Summary
CREATE OR REPLACE VIEW lakehouse.gold.v_pipeline_audit_summary AS
SELECT
    dag_id,
    step_name,
    status,
    count(*) AS run_count,
    sum(records_processed) AS total_records_processed,
    max(created_at) AS last_run_at
FROM lakehouse.gold.pipeline_audit
GROUP BY dag_id, step_name, status;

-- 5. Fiat & Stablecoin Reference Rates (USDT, USDC Peg Monitoring)
CREATE OR REPLACE VIEW lakehouse.gold.v_fiat_stablecoin_rates AS
SELECT
    timestamp,
    exchange,
    raw_symbol,
    canonical_symbol,
    base_currency AS stablecoin,
    quote_currency AS fiat,
    close AS rate_to_usd,
    volume,
    trade_count,
    last_updated_at
FROM lakehouse.gold.curated_crypto_bars
WHERE canonical_symbol IN ('USDT-USD', 'USDC-USD');

-- 7. Daily Normalized Cross-Exchange Spread (Fast Analytical Serving from Materialized Aggregates)
CREATE OR REPLACE VIEW lakehouse.gold.v_daily_cross_exchange_normalized_spread AS
SELECT
    s.base_currency,
    s.trade_date,
    s.alpaca_close AS alpaca_close_usd,
    s.alpaca_volume,
    s.binance_close AS binance_close_raw,
    s.binance_volume,
    coalesce(r.rate_to_usd, 1.0) AS usdt_rate_to_usd,
    round(s.binance_close * coalesce(r.rate_to_usd, 1.0), 4) AS binance_close_usd_adjusted,
    round(s.alpaca_close - (s.binance_close * coalesce(r.rate_to_usd, 1.0)), 4) AS normalized_spread_abs,
    round(((s.alpaca_close - (s.binance_close * coalesce(r.rate_to_usd, 1.0))) / (s.binance_close * coalesce(r.rate_to_usd, 1.0))) * 100, 4) AS normalized_spread_pct,
    s.last_updated_at
FROM lakehouse.gold.agg_daily_cross_exchange_spread s
LEFT JOIN (
    SELECT trade_date, close_price AS rate_to_usd
    FROM lakehouse.gold.agg_daily_crypto_ohlcv
    WHERE canonical_symbol = 'USDT-USD'
) r
  ON r.trade_date = s.trade_date;


