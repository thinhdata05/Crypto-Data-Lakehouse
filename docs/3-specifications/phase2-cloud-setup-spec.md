# Đặc Tả Kiến Trúc Phase 2: Đề Xuất Thiết Lập Hạ Tầng Cloud & Triển Khai Sản Xuất (AWS)

> [!IMPORTANT]
> **Tài liệu đề xuất kiến trúc Cloud (Cloud Architectural Proposal & Reference)**:
> Tài liệu này là **bản đặc tả thiết kế kiến trúc chuẩn** cho Phase 2 (triển khai trên AWS Cloud). 
> Repository Local hiện tại chỉ tập trung vào vai trò **Lightweight Producer** (kết thúc tại `lakehouse.silver.bars` $\to$ xuất bản lát cắt Parquet và `_manifest.json` vào `confirmed/`). Toàn bộ kiến trúc Cloud được thiết kế độc lập dưới dạng AWS Managed Services (IaC / Terraform) và không phụ thuộc vào codebase của Local. Khi triển khai Cloud thật, hệ thống sẽ sử dụng hạ tầng AWS được đề xuất trong tài liệu này cùng pipeline/IaC độc lập.

> **Phạm vi đặc tả**: Hướng dẫn chi tiết thiết kế hạ tầng AWS (S3 Lifecycle, AWS Glue Data Catalog, IAM Least-Privilege, Athena Serving Views) và mô hình dịch vụ đám mây cho các thành phần: **Realtime Ingestion Service** và **Single-Writer Convergence Service** nhằm hiện thực hóa mô hình **2 Medallion Song Song Hội Tụ Tại Cloud Silver (`silver.bars`)**.

---

## 1. Mục Tiêu & Mô Hình Dịch Vụ Đám Mây (Target Cloud Architecture)

Mô hình phân tách ranh giới rõ ràng giữa Local Producer và Cloud Consumer:

```text
[LOCAL MACHINE: Lightweight Producer]                 [AWS CLOUD INFRASTRUCTURE: Single Source of Truth]
raw/ ─► staging/canonical/                            S3: s3://<cloud>-lakehouse/confirmed/ (TTL 7 days)
     ─► bronze.crypto_bars ─► silver.bars                       │
(Dừng tại silver.bars, KHÔNG tạo Gold)                          ▼
       │                                              Single-Writer Convergence Service
       ▼                                              (AWS Lambda / EMR Serverless / ECS)
publish_confirmed_parquet ── (IAM: confirmed_publisher) ───────►│
                                                                │
           WS Alpaca + WS Binance ──► ECS Fargate Consumer ────►│
                                      (bronze.rt_bars)          ▼
                                                  Glue Catalog / Iceberg silver.bars
                                                                │
                                                                ▼
                                                  Athena / QuickSight / ML Serving (gold.*)
```

---

## 2. Thiết Kế Hạ Tầng AWS S3 & Vòng Đời Lưu Trữ (Cost-Optimized Lifecycle Rules)

### A. Cấu Trúc Prefix Trên S3 Bucket (`s3://<company>-crypto-lakehouse/`)
```text
s3://<company>-crypto-lakehouse/
├── confirmed/               # Tầng đệm nhận lát cắt Parquet từ Local (batch_id=*/_manifest.json)
├── ops/                     # Control Plane: ingestion_manifest/, pipeline_audit/
├── bronze/
│   └── rt_bars/             # Dữ liệu realtime từ WebSocket/Kafka (WS Alpaca + WS Binance)
├── silver/
│   └── bars/                # BẢNG HỘI TỤ CHÍNH (Single Writer hợp nhất rt_bars + confirmed/)
└── gold/                    # Single Source of Truth cho toàn bộ tầng phục vụ phân tích
    ├── dim_symbol/
    ├── dim_date/
    ├── agg_daily_crypto_ohlcv/
    └── agg_daily_cross_exchange_spread/
```

### B. AWS S3 Lifecycle Rules
1. **Prefix `confirmed/` (TTL 7 ngày)**:
   - *Nguyên tắc*: Khi file Parquet và `_manifest.json` được Local đẩy lên, tiến trình Cloud Single-Writer Convergence Service (EMR Serverless / Glue / ECS) đọc, xác thực SHA-256 và hợp nhất vào `silver.bars` trong vòng vài phút.
   - *Hành động*: Cấu hình Lifecycle Rule tự động xóa toàn bộ object trong `confirmed/` sau **7 ngày** (`Expiration: 7 days`).
2. **Prefix `bronze/rt_bars/` & `silver/bars/`**:
   - Các file snapshot/metadata cũ hết hạn được dọn dẹp định kỳ bởi `expire_snapshots` (`retain_last => 2`).
   - Dữ liệu Parquet phân vùng theo `days(timestamp)` lưu trữ dạng `S3 Standard` cho 30 ngày gần nhất, chuyển sang `S3 Standard-Infrequent Access (IA)` sau 30 ngày.

---

## 3. Chính Sách Bảo Mật IAM Phân Quyền Tối Thiểu (Least Privilege)

Hệ thống phân tách nghiêm ngặt 2 vai trò IAM độc lập để loại bỏ hoàn toàn nguy cơ Local can thiệp vào Cloud Catalog:

### Role 1: `confirmed_publisher_role` (Gán cho Local Producer)
- **Mục đích**: Chỉ cho phép Local ghi các file Parquet lát cắt và file `_manifest.json` vào thư mục `confirmed/`.
- **Tuyệt đối cấm**: Không có quyền ghi vào `bronze/`, `silver/`, `gold/`, không có quyền can thiệp vào AWS Glue Catalog hay Athena.
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowLocalToPublishConfirmedParquet",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::<company>-crypto-lakehouse/confirmed/*"
    },
    {
      "Sid": "DenyEverythingElse",
      "Effect": "Deny",
      "Action": [
        "glue:*",
        "athena:*",
        "s3:DeleteObject"
      ],
      "Resource": "*"
    }
  ]
}
```

### Role 2: `cloud_silver_writer_role` (Gán cho Cloud Convergence Worker / ECS)
- **Mục đích**: Đọc dữ liệu từ `confirmed/*`, đọc/ghi vào `lakehouse.bronze.rt_bars`, `lakehouse.silver.bars`, `lakehouse.gold.*`, và cập nhật AWS Glue Data Catalog.

---

## 4. Đề Xuất Dịch Vụ Triển Khai Cloud & Ghi Chú Kỹ Thuật (AWS Managed Services Proposal)

> [!NOTE]
> Hệ thống Cloud được triển khai độc lập thông qua hạ tầng AWS Managed Services và pipeline/IaC riêng biệt. Dưới đây là kiến trúc dịch vụ AWS được đề xuất cho production Cloud.

### A. Dual-Exchange Realtime Consumer (AWS ECS Fargate Task)
- Chạy dưới dạng AWS ECS Fargate Task (0.5 vCPU, 1 GB RAM) kết nối đồng thời **2 luồng WebSocket**:
  1. **Alpaca Crypto WebSocket** (`wss://stream.data.alpaca.markets/v1beta3/crypto/us`)
  2. **Binance Public Combined Kline Stream** (`wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m/...` — hoàn toàn miễn phí, không cần API key).
- **Ghi chú kỹ thuật bắt buộc khi xây dựng consumer service trên Cloud**:
  - Cần bổ sung bộ giải mã (Parser) cho gói tin JSON lồng nhau của **Binance WebSocket (`@kline_1m`)**:
    ```json
    {
      "e": "kline",
      "E": 1730900060000,
      "s": "BTCUSDT",
      "k": {
        "t": 1730900000000,
        "o": "70010.00",
        "h": "70060.00",
        "l": "69960.00",
        "c": "70030.00",
        "v": "35.120",
        "n": 520,
        "x": true
      }
    }
    ```
  - Chỉ thu nhận bản ghi khi nến 1 phút đã đóng (`k["x"] == True`), ánh xạ `k["t"]` (ms) sang `timestamp` UTC và gán `exchange="binance"`, `source="realtime"`, `priority=1`.
  - Đối với Alpaca, chuẩn hóa `raw_symbol` về dạng gạch dưới (`BTC_USD`) qua `clean_raw_symbol(sym).replace("/", "_")` để khớp 100% với khóa `(exchange, raw_symbol, timestamp)` từ `confirmed/`.

### B. Cloud Silver Single-Writer Convergence Service (EMR Serverless / AWS Glue / ECS)
- Kích hoạt bởi S3 Event Notification khi có file `_manifest.json` xuất hiện trong `s3://.../confirmed/`, hoặc định kỳ mỗi 5 phút.
- Hợp nhất `bronze.rt_bars` (`priority IN (1, 3)`) + `confirmed/` (`priority = 2`) vào `silver.bars`, sau đó cập nhật `gold.dim_symbol`, `gold.agg_daily_crypto_ohlcv`, `gold.agg_daily_cross_exchange_spread` và bộ 7 Views phân tích (được đặc tả chi tiết tại **Mục 5** của tài liệu này).

### C. Cold Bootstrap 1 Lần Duy Nhất Lên Cloud
- Toàn bộ **104,657,836 dòng** lịch sử đã làm sạch trong Local `lakehouse.silver.bars` (`2,058` file Parquet theo ngày) sẽ được Cold Bootstrap 1 lần duy nhất lên Cloud `silver.bars` khi triển khai Phase 2 trước khi bật luồng Incremental `confirmed/`.

---

## 5. Đặc Tả DDL 7 Gold Analytical Views Trên Cloud (Reference DDL)

Dưới đây là định nghĩa SQL DDL tham khảo của 7 Views phân tích phục vụ truy vấn nghiệp vụ và serving trên hạ tầng Cloud (AWS Athena / Spark SQL Catalog):

```sql
-- ==============================================================================
-- Cloud Gold Analytics & Reporting Views (Reference DDL)
-- Database: lakehouse.gold
-- ==============================================================================

-- 1. Daily Aggregated OHLCV Serving View
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

-- 2. Hourly Aggregated OHLCV Rollup per Exchange (Backed by converged Cloud silver.bars)
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
FROM lakehouse.silver.bars
GROUP BY exchange, canonical_symbol, base_currency, quote_currency, date_trunc('hour', timestamp);

-- 3. Daily Cross-Exchange Comparison View
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

-- 4. Backwards compatibility alias for daily cross exchange
CREATE OR REPLACE VIEW lakehouse.gold.v_cross_exchange_comparison AS
SELECT * FROM lakehouse.gold.v_cross_exchange_daily_spread;

-- 5. Pipeline Audit Summary (Backed by Control Plane lakehouse.ops.pipeline_audit)
CREATE OR REPLACE VIEW lakehouse.gold.v_pipeline_audit_summary AS
SELECT
    dag_id,
    step_name,
    status,
    count(*) AS run_count,
    sum(records_processed) AS total_records_processed,
    max(created_at) AS last_run_at
FROM lakehouse.ops.pipeline_audit
GROUP BY dag_id, step_name, status;

-- 6. Fiat & Stablecoin Reference Rates (USDT, USDC Peg Monitoring from converged silver.bars)
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
    ingested_at AS last_updated_at
FROM lakehouse.silver.bars
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
```
