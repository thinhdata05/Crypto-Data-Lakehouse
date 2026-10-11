# Data Contract: Symbol Registry, Schemas & Currency Invariants

> **Target Platform**: Multi-Exchange Crypto Lakehouse Platform  
> **Enforcement Layer**: Schema validation in Python, Spark, DuckDB, Iceberg DDL  
> **Scope**: 68 Conformed Market Symbols across Alpaca and Binance (2021 – 2026)

---

## 1. Core Data Contract Invariants

### A. The Canonical Merge Key (Business Key)
The unique identifier across all Lakehouse tiers is strictly:
$$\text{Business Key} = (\text{exchange}, \text{raw\_symbol}, \text{timestamp})$$

* **Multi-Exchange Independence**:
  - Alpaca and Binance are distinct exchanges with independent order books, liquidity pools, and matching engines.
  - A record for `BTC/USD` on Alpaca and `BTCUSDT` on Binance at the exact same second is **NEVER a duplicate** and must **NEVER overwrite each other**.
* **Intra-Exchange Priority Conflict Resolution**:
  - Priority resolution ($3 > 2 > 1$) applies strictly **within the same exchange**.
  - Historical batch backfill: `priority = 2`.
  - Real-time streaming bar: `priority = 1`.
  - Late-arriving exchange correction ($T='u'$): `priority = 3`.
* **No Synthetic / Artificial Rows**:
  - Tokens begin strictly at their real exchange listing date ($\ge 2021-01-01$ UTC).
  - Pipeline must never fabricate missing minutes or synthesize artificial records for tokens prior to listing.

---

## 2. Conformed Canonical Schema

All downstream tables in Silver and Gold adhere to the following conformed schema:

| Column Name | Data Type | Nullable | Description | Example |
| :--- | :--- | :---: | :--- | :--- |
| `exchange` | `STRING` | NO | Source exchange identifier (`alpaca`, `binance`) | `'binance'` |
| `raw_symbol` | `STRING` | NO | Native symbol as formatted by the exchange API | `'BTCUSDT'` |
| `timestamp` | `TIMESTAMP` | NO | Bar opening timestamp in UTC (microsecond precision) | `'2024-09-04 12:00:00'` |
| `open` | `DOUBLE` | NO | Opening price of the bar | `58250.00` |
| `high` | `DOUBLE` | NO | Highest price during the bar interval | `58320.50` |
| `low` | `DOUBLE` | NO | Lowest price during the bar interval | `58210.10` |
| `close` | `DOUBLE` | NO | Closing price of the bar | `58290.00` |
| `volume` | `DOUBLE` | NO | Base currency volume traded during the bar interval | `42.158` |
| `trade_count` | `BIGINT` | NO | Number of trades executed during the bar interval | `1842` |
| `source` | `STRING` | NO | Ingestion source (`'historical'`, `'incremental'`, `'realtime'`) | `'historical'` |
| `priority` | `INT` | NO | Priority rating ($3 > 2 > 1$) for merge conflict resolution | `2` |
| `base_currency` | `STRING` | NO | Asset token symbol | `'BTC'` |
| `quote_currency`| `STRING` | NO | Pricing currency (retained natively without conversion) | `'USDT'` |
| `canonical_symbol`| `STRING` | NO | Normalized hyphenated symbol for cross-exchange joins | `'BTC-USDT'` |
| `batch_id` | `STRING` | NO | Ingestion run UUID for lineage and auditing | `'4caa5174-...'` |
| `last_updated_at` | `TIMESTAMP` | NO | Engine timestamp when record was committed to Iceberg | `'2026-09-16 07:02:58'` |

---

## 3. Registered Market Symbols (68 Total)

The Lakehouse tracks exactly 34 core assets across both exchanges (34 on Alpaca + 34 on Binance = 68 total).

### A. Quote Currency Distribution & Scope Pruning
To prevent liquidity fragmentation and 60%+ redundant I/O, the ingestion engine selects exactly **1 preferred quote currency per symbol per exchange** according to the priority hierarchy:
$$\text{Binance}: \text{USDT} > \text{USDC} > \text{USD} > \text{BTC} \quad\mid\quad \text{Alpaca}: \text{USD} > \text{USDT} > \text{USDC}$$

#### Distribution in `lakehouse.gold.dim_symbol`:
- **Alpaca (34)**:
  - 23 pairs quoted in **USD** (`BTC-USD`, `ETH-USD`, `SOL-USD`, `ADA-USD`, `AVAX-USD`, etc.)
  - 7 pairs quoted in **USDT** (`DOGE-USDT`, `LINK-USDT`, `LTC-USDT`, `SHIB-USDT`, `SUSHI-USDT`, `UNI-USDT`, `YFI-USDT`)
  - 4 pairs quoted in **USDC** (`CRV-USDC`, `DOT-USDC`, `GRT-USDC`, `XTZ-USDC`)
- **Binance (34)**:
  - 28 pairs quoted in **USDT** (`BTC-USDT`, `ETH-USDT`, `SOL-USDT`, `BNB-USDT`, `AVAX-USDT`, etc.)
  - 4 pairs quoted in **USDC** (`CRV-USDC`, `DOT-USDC`, `GRT-USDC`, `XTZ-USDC` — matched 1:1 with Alpaca)
  - 1 pair quoted in **USD** (`USDC-USD`)
  - 1 pair quoted in **TUSD** (`USD-TUSD`)

---

## 4. Currency Peg & Normalized Spread Contract

### A. Non-Equivalence Principle
* `USD` (Fiat central bank currency) $\ne$ `USDT` (Tether stablecoin) $\ne$ `USDC` (Circle stablecoin).
* Native table measures in Bronze/Silver/Gold must **never** apply in-flight currency conversions.

### B. Normalized Spread View Contract
When performing cross-exchange analytics or triggering arbitrage alerts, downstream consumers must query `lakehouse.gold.v_cross_exchange_normalized_spread`:
$$\text{Adjusted Binance Price (USD)} = \text{Close}_{\text{Binance}} \times \text{Rate}_{\text{USDT/USD}}$$
$$\text{Normalized Spread} = \text{Close}_{\text{Alpaca (USD)}} - \text{Adjusted Binance Price (USD)}$$

---

## 5. Data Quality Validation Rules

Every ingestion run (Historical or Incremental) must pass 100% of the following audit gates before sign-off:
1. **Uniqueness**: `COUNT(*) = COUNT(DISTINCT exchange, raw_symbol, timestamp)` $\to$ 0 duplicate keys.
2. **Price Sanity**: $high \ge low$, $open > 0$, $close > 0$, $high \ge open$, $high \ge close$, $low \le open$, $low \le close$.
3. **Volume Sanity**: $volume \ge 0$, $trade\_count \ge 0$.
4. **Audit Trail**: Every execution must write run statistics to `lakehouse.gold.pipeline_audit`.
