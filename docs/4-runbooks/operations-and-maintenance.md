# Runbook: Operations, Incremental Pipeline & Lakehouse Maintenance

> **Target Platform**: Multi-Exchange Crypto Lakehouse Platform (Apache Iceberg, Spark, MinIO, Airflow)  
> **Audience**: Platform Engineers, Data Engineers, SRE

---

## 1. Quick Service Management (Docker Compose)

### A. Health & Status Verification
Check all core Lakehouse services:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```
All critical containers should be `Up` and `healthy`:
- `lakehouse-minio` (Ports 9000: API, 9001: Console)
- `lakehouse-iceberg-rest` (Port 8181: REST Catalog)
- `lakehouse-spark-iceberg` (Port 8080: Spark UI, 8888: Jupyter)
- `lakehouse-postgres` (Port 5432: Airflow Metadata DB)
- `lakehouse-airflow-webserver` (Port 8085)
- `lakehouse-airflow-scheduler`

### B. Graceful Restart
```bash
# Restart without losing MinIO or Postgres state
docker compose restart

# Recreate a specific container if configuration changes
docker compose up -d --force-recreate spark-iceberg
```

---

## 2. Running Incremental Hourly Sync

The incremental pipeline fetches 1-minute bars via REST API from Binance and Alpaca and performs an ACID `MERGE INTO` Iceberg Curated Gold.

### A. Manual Airflow DAG Trigger
Trigger via Airflow CLI inside the webserver container:
```bash
docker exec lakehouse-airflow-webserver airflow dags trigger lakehouse_incremental_hourly
```
Or open the Airflow UI at `http://localhost:8085`, unpause `lakehouse_incremental_hourly`, and click **Trigger DAG**.

### B. Verify Execution Pipeline
Check task execution progression:
```bash
docker exec lakehouse-airflow-webserver airflow dags list-runs -d lakehouse_incremental_hourly
```
Key tasks in the DAG:
1. `build_hourly_window`: Computes UTC window interval and generates UUID batch ID.
2. `fetch_rest_bars`: Fetches minute bars (`1m`/`1Min`) via REST API to staging.
3. `incremental_raw_to_bronze`: Appends batch rows to `lakehouse.bronze.crypto_bars`.
4. `incremental_bronze_to_silver`: Cleanses data and assigns `priority=2`.
5. `incremental_reconcile_merge_sync`: Executes `MERGE INTO` on `(exchange, raw_symbol, timestamp)` into Curated Silver and Gold.
6. `materialize_gold_aggregates`: Refreshes daily rollups in Gold.
7. `validate_incremental`: Runs post-ingestion audit and ensures zero duplicate keys.

---

## 3. Apache Iceberg Table Maintenance & Snapshot Cleanup

Over time, frequent batch/incremental commits create numerous table snapshots and small metadata manifests. Periodic maintenance is required to prevent query degradation and lower Cloud storage costs.

### A. Check Current Snapshot Count
```bash
docker exec lakehouse-spark-iceberg python3 -c "
from pyiceberg.catalog import load_catalog
cat = load_catalog('lakehouse', **{'type':'rest','uri':'http://rest:8181','s3.endpoint':'http://minio:9000','s3.access-key-id':'admin','s3.secret-access-key':'password'})
t = cat.load_table('gold.curated_crypto_bars')
print('Snapshots count:', len(t.snapshots()))
"
```

### B. Expire Old Snapshots (Retain Latest 2)
Execute the Iceberg Spark maintenance stored procedure:
```bash
docker exec lakehouse-spark-iceberg spark-sql -e "
CALL lakehouse.system.expire_snapshots(
    table => 'gold.curated_crypto_bars',
    retain_last => 2
);
"
```

### C. Parquet Data File Compaction (Rewrite Small Files)
Rewrite small partition files into optimized 128MB–256MB Parquet chunks:
```bash
docker exec lakehouse-spark-iceberg spark-sql -e "
CALL lakehouse.system.rewrite_data_files(
    table => 'gold.curated_crypto_bars',
    strategy => 'binpack',
    options => map('target-file-size-bytes','134217728')
);
"
```

---

## 4. Querying Analytical & Normalized Spread Views

### A. List Analytical Views
```bash
docker exec lakehouse-spark-iceberg spark-sql -e "SHOW VIEWS IN lakehouse.gold;"
```
Views available:
- `v_daily_crypto_ohlcv`: Daily OHLCV rollups per exchange.
- `v_hourly_crypto_ohlcv`: Hourly OHLCV rollups per exchange.
- `v_cross_exchange_daily_spread`: Pre-materialized daily cross-exchange spread.
- `v_daily_cross_exchange_normalized_spread`: Materialized daily spread adjusted for stablecoin peg deviations (< 1s response).
- `v_fiat_stablecoin_rates`: Reference rates for USDT and USDC to USD.

### B. Sample Query: Normalized Arbitrage Serving
```sql
-- 1. Fast analytical serving view (< 1s):
SELECT 
    base_currency, 
    trade_date, 
    alpaca_close_usd, 
    binance_close_raw, 
    usdt_rate_to_usd, 
    binance_close_usd_adjusted, 
    normalized_spread_abs, 
    normalized_spread_pct
FROM lakehouse.gold.v_daily_cross_exchange_normalized_spread 
WHERE base_currency = 'BTC'
ORDER BY trade_date DESC LIMIT 10;
```

For high-frequency minute-level spreads with pushdown partition safety:
```bash
docker exec lakehouse-spark-iceberg python src/serving/query_minute_spread.py \
    --base-currency BTC \
    --start-time 2026-08-10T00:00:00Z \
    --end-time 2026-08-11T00:00:00Z
```

---

## 5. Troubleshooting Common Issues

### Issue 1: MinIO or Spark Container OOM
- **Cause**: Spark Driver heap memory exhausted by unconstrained fanout or excessive concurrent partition scans.
- **Remedy**: Ensure PyIceberg + DuckDB zero-JVM streaming path is used for symbol batching, and verify `write.distribution-mode = 'range'` in DDL.

### Issue 2: Docker Mount Type Mismatch on Windows
- **Symptom**: `OCI runtime create failed: error mounting host path ... not a directory`.
- **Remedy**: Check if host file path accidentally became a folder during Docker daemon crash. Run `docker compose up -d --force-recreate <service>`.
