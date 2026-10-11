-- ==============================================================================
-- Gold Analytics & Reporting Views
-- Catalog: lakehouse
-- In accordance with PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md
-- ==============================================================================

-- 1. Daily Aggregated OHLCV Rollup per Exchange and Canonical Symbol
CREATE OR REPLACE VIEW lakehouse.gold.v_daily_crypto_ohlcv AS
SELECT
    exchange,
    canonical_symbol,
    base_currency,
    quote_currency,
    to_date(timestamp) AS trade_date,
    min_by(open, timestamp) AS open_price,
    max(high) AS high_price,
    min(low) AS low_price,
    max_by(close, timestamp) AS close_price,
    sum(volume) AS total_volume,
    sum(trade_count) AS total_trades,
    count(*) AS minute_bars_count,
    max(last_updated_at) AS last_updated_at
FROM lakehouse.gold.curated_crypto_bars
GROUP BY exchange, canonical_symbol, base_currency, quote_currency, to_date(timestamp);

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

-- 3. Cross-Exchange Comparison View (Alpaca vs Binance for concurrent minutes)
CREATE OR REPLACE VIEW lakehouse.gold.v_cross_exchange_comparison AS
SELECT
    a.base_currency,
    a.timestamp,
    a.close AS alpaca_close,
    a.volume AS alpaca_volume,
    b.close AS binance_close,
    b.volume AS binance_volume,
    (a.close - b.close) AS spread_abs,
    round(((a.close - b.close) / b.close) * 100, 4) AS spread_pct
FROM lakehouse.gold.curated_crypto_bars a
JOIN lakehouse.gold.curated_crypto_bars b
  ON a.base_currency = b.base_currency
  AND a.timestamp = b.timestamp
WHERE a.exchange = 'alpaca' AND b.exchange = 'binance';

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

-- 6. Normalized Cross-Exchange Spread (Eliminating Stablecoin De-peg Phantom Arbitrage)
CREATE OR REPLACE VIEW lakehouse.gold.v_cross_exchange_normalized_spread AS
SELECT
    a.base_currency,
    a.timestamp,
    a.close AS alpaca_close_usd,
    a.volume AS alpaca_volume,
    b.close AS binance_close_raw,
    b.volume AS binance_volume,
    b.quote_currency AS binance_quote,
    coalesce(r.rate_to_usd, 1.0) AS stablecoin_rate_to_usd,
    round(b.close * coalesce(r.rate_to_usd, 1.0), 4) AS binance_close_usd_adjusted,
    round(a.close - (b.close * coalesce(r.rate_to_usd, 1.0)), 4) AS normalized_spread_abs,
    round(((a.close - (b.close * coalesce(r.rate_to_usd, 1.0))) / (b.close * coalesce(r.rate_to_usd, 1.0))) * 100, 4) AS normalized_spread_pct
FROM lakehouse.gold.curated_crypto_bars a
JOIN lakehouse.gold.curated_crypto_bars b
  ON a.base_currency = b.base_currency
  AND a.timestamp = b.timestamp
LEFT JOIN (
    SELECT timestamp, base_currency, close AS rate_to_usd
    FROM lakehouse.gold.curated_crypto_bars
    WHERE canonical_symbol IN ('USDT-USD', 'USDC-USD')
) r
  ON r.timestamp = b.timestamp
  AND r.base_currency = b.quote_currency
WHERE a.exchange = 'alpaca' AND b.exchange = 'binance';

