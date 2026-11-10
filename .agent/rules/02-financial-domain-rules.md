---
description: Financial market conventions, quote currency preservation, stablecoin peg normalization, and OHLCV sanity rules
globs: "src/preprocessing/**, src/pipeline/**, sql/**, src/serving/**"
---

# 02. Financial Domain & Currency Standardization Rules

Domain rules governing multi-exchange market microstructure, quote currency preservation, stablecoin de-peg risk, and OHLCV mathematical sanity.

---

## 1. Multi-Exchange Liquidity & Pricing Independence
- **Alpaca Crypto**:
  - Primary quote currency is US Dollar fiat (`USD`, plus selected `USDT`/`USDC` pairs).
  - Order book depth and matching follow US-regulated crypto venue dynamics.
- **Binance**:
  - Primary quote currency is stablecoin (`USDT`, `USDC`, plus `USD`/`TUSD`).
  - Represents global offshore crypto liquidity (50x+ higher BTC volume than Alpaca).
- **Pricing Rule**:
  - Price differences between Alpaca and Binance for the same base asset at the same minute reflect genuine market basis, fee structures, and stablecoin FX rates—never treat cross-exchange price divergence as a data error.

---

## 2. Quote Currency Preservation in Base Physical Tables
- Across all physical Medallion tables:
  - `lakehouse.bronze.crypto_bars`
  - `lakehouse.bronze.rt_bars`
  - `lakehouse.silver.bars`
  - `lakehouse.gold.agg_daily_crypto_ohlcv`
- **MANDATORY RULE**:
  - Store `open`, `high`, `low`, `close` strictly in their native `quote_currency` (`USD`, `USDT`, `USDC`, `TUSD`).
  - **NEVER multiply or convert `USDT` to `USD` inside base physical tables.**
  - Rationale: In-flight FX conversion destroys auditability and corrupts historical prices because stablecoin exchange rates (`USDT/USD`, `USDC/USD`) fluctuate every minute.

---

## 3. Primary Quote Currency Pruning Hierarchy
- To prevent liquidity fragmentation and combinatorial cross-join explosion, the preprocessors (`src/preprocessing/alpaca.py`, `src/preprocessing/binance.py`) select **1 primary quote pair per base asset per exchange**:
  $$\text{Binance}: \mathbf{USDT} > \mathbf{USDC} > \mathbf{USD} > \mathbf{BTC} \quad\mid\quad \text{Alpaca}: \mathbf{USD} > \mathbf{USDT} > \mathbf{USDC}$$
- Result: Exactly **34 symbols on Alpaca + 34 symbols on Binance = 68 canonical symbol pairs**.
- **Raw Symbol Formatting Invariant**:
  - Alpaca `raw_symbol` in storage is normalized with an underscore (e.g., `BTC_USD`, `ETH_USD`, `AAVE_USD`) via `clean_raw_symbol(sym).replace("/", "_")` in both `alpaca.py` and `rest_fetcher.py`.
  - Binance `raw_symbol` is stored concatenated (e.g., `BTCUSDT`, `ETHUSDT`).

---

## 4. Normalized Currency Conversion at the Serving Layer
- Cross-exchange currency normalization is performed **exclusively at the Serving Layer**:
  - **View `lakehouse.gold.v_fiat_stablecoin_rates`**: Extracts daily/minute `USDT-USD` and `USDC-USD` conversion rates from `silver.bars`.
  - **View `lakehouse.gold.v_daily_cross_exchange_normalized_spread`** and **Bounded Minute API `src/serving/query_minute_spread.py`**:
    $$\text{normalized\_binance\_close} = \text{binance\_close} \times \text{coalesce(usdt\_usd\_rate, 1.0)}$$
    $$\text{normalized\_spread\_usd} = \text{alpaca\_close} - \text{normalized\_binance\_close}$$
    $$\text{normalized\_spread\_pct} = \left(\frac{\text{normalized\_spread\_usd}}{\text{normalized\_binance\_close}}\right) \times 100$$

---

## 5. 7-Rule OHLCV Mathematical Sanity Contract
- Every 1-minute bar admitted into `lakehouse.silver.bars` must satisfy all 7 OHLCV predicates (`OHLCV_SANITY_SQL_PREDICATE` in `src/preprocessing/normalizer.py`, `src/pipeline/silver/bronze_to_silver.py`, and `src/pipeline/quality/data_validation.py`):
  1. $\text{high} \ge \text{low}$
  2. $\text{high} \ge \text{open}$
  3. $\text{high} \ge \text{close}$
  4. $\text{low} \le \text{open}$
  5. $\text{low} \le \text{close}$
  6. Non-negative activity: $\text{volume} \ge 0 \land (\text{trade\_count IS NULL} \lor \text{trade\_count} \ge 0)$
  7. Positive prices: $\text{open} > 0 \land \text{high} > 0 \land \text{low} > 0 \land \text{close} > 0$
- Records failing any rule (including negative trade count or price envelope violations) are filtered out during the `bronze.crypto_bars` $\to$ `silver.bars` transformation (`48,982` invalid/duplicate rows filtered out of `104,706,818` Bronze rows).

---

## 6. Temporal Integrity & Zero Synthetic Rows
- Historical data for each symbol begins strictly at its real exchange listing date ($\ge 2021-01-01$ UTC).
- **Strictly Forbidden**:
  - Never generate synthetic/dummy bars or forward-fill missing minutes prior to listing or during zero-volume intervals.
