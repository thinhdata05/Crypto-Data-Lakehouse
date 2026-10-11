# PROJECT MEMORY: Multi-Exchange Crypto Lakehouse Platform

## 1. Project Identity & Status
- **Project**: Multi-Exchange Crypto Lakehouse Platform (Apache Iceberg, Spark, MinIO, DuckDB, PyIceberg, Airflow)
- **Current Phase**: **Phase 1 Complete (Authoritative Core & Historical Ingestion)**
- **Total Records in Gold**: **100,650,539 bars** (100% verified, 0 duplicate keys, 0 OHLCV sanity violations)
- **Covered Exchanges**: **Alpaca** (34 symbols, quote `USD`) & **Binance** (34 symbols, quote `USDT` / `USDC` / `USD`)
- **Conformed Canonical Symbols**: 34 high-liquidity crypto assets (`BTC`, `ETH`, `SOL`, `ADA`, `AVAX`, `DOGE`, etc.)
- **Status of Services**: All 6 Docker containers (`lakehouse-minio`, `lakehouse-iceberg-rest`, `lakehouse-spark-iceberg`, `lakehouse-postgres`, `lakehouse-airflow-webserver`, `lakehouse-airflow-scheduler`) healthy on network `lakehouse-net`.

---

## 2. Directory & Documentation Index

### Core Documentation Tree (`docs/`)
- [`README.md`](README.md): Tổng quan kiến trúc, pipeline, bảng dữ liệu, và lệnh khởi động nhanh.
- [`docs/1-architecture/storage-and-warehouse.md`](docs/1-architecture/storage-and-warehouse.md):
  - Phân định rõ ràng: MinIO Object Storage (`s3://warehouse/`) là nơi lưu trữ Iceberg data & metadata.
  - Thư mục local `warehouse/` chỉ là scratch / staging tạm thời (`warehouse/staging/canonical`).
- [`docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md`](docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md):
  - RCA lỗi Spark OOM `PartitionedFanoutWriter` (1,584 partition days x 64MB = 101GB memory pressure).
  - Kiến trúc Hybrid PyIceberg + DuckDB streaming (giảm RAM < 2.8GB, tốc độ 71k - 129k rows/s).
  - Phân tích rủi ro de-peg Stablecoin (USDT vs USDC vs USD) và giải pháp serving view chuẩn hóa.
- [`docs/3-specifications/phase1-migration-spec.md`](docs/3-specifications/phase1-migration-spec.md):
  - Bản đặc tả kỹ thuật chi tiết của toàn bộ hệ thống Phase 1.
- [`docs/4-runbooks/operations-and-maintenance.md`](docs/4-runbooks/operations-and-maintenance.md):
  - Sổ tay vận hành: Khởi động container, chạy Airflow DAGs, Spark SQL CLI, compaction & snapshot expiration.
- [`docs/5-data-contracts/symbol-and-currency-contract.md`](docs/5-data-contracts/symbol-and-currency-contract.md):
  - Data contracts: Quy ước đặt tên symbol, thứ tự ưu tiên Quote Currency, và các ràng buộc Invariants.

---

## 3. Core Architectural Invariants (DO NOT VIOLATE)

1. **Multi-Exchange Independence**:
   - Alpaca và Binance có sổ lệnh (order book) và cơ chế khớp lệnh độc lập.
   - Dữ liệu cùng `timestamp` và cùng `canonical_symbol` (ví dụ `BTC` lúc `2024-09-04 12:00:00 UTC` trên Alpaca và Binance) là **2 thực thể riêng biệt**, KHÔNG ĐƯỢC gộp hay ghi đè lên nhau.
2. **Expanded Business Key**:
   - Khóa định danh hợp nhất duy nhất trên toàn hệ thống: `(exchange, raw_symbol, timestamp)`.
3. **Intra-Exchange Priority Resolution**:
   - Cơ chế giải quyết xung đột ưu tiên (`priority`: 3 = Backfill sửa lỗi > 2 = REST API Hourly > 1 = Realtime Websocket) **CHỈ áp dụng nội bộ trong cùng 1 exchange**.
4. **No Synthetic / Artificial Rows**:
   - Dữ liệu lịch sử của mỗi symbol bắt đầu chính xác từ thời điểm niêm yết thực tế trên sàn đó ($\ge 2021-01-01$ UTC).
   - Tuyệt đối không sinh dòng giả (forward-fill / dummy records) cho các mốc thời gian trước khi niêm yết.
5. **Quote Currency Preservation in Base Storage**:
   - Bảng cơ sở (Bronze, Silver, Curated Silver, Gold) lưu trữ đúng giá trị nguyên bản theo đồng yết giá (`quote_currency`: Alpaca là `USD`, Binance là `USDT`/`USDC`).
   - Tuyệt đối **không tự ý nhân tỷ giá để đổi `USDT` thành `USD`** trong bảng gốc nhằm bảo toàn tính toàn vẹn kiểm toán (auditability).
   - Mọi quy đổi chuẩn hóa tỷ giá chỉ được thực hiện ở tầng phục vụ phân tích (Serving Layer View: `gold.v_cross_exchange_normalized_spread`).
6. **Iceberg Partitioning Spec**:
   - Tất cả các bảng `crypto_bars` đều phân vùng theo `days(timestamp)`. Không phân vùng theo symbol để tránh bùng nổ small files.

---

## 4. Current Lakehouse State & Metrics

### Physical Storage & Tables
- **Iceberg Warehouse**: `s3://warehouse/` (MinIO: `http://localhost:9000`, bucket: `warehouse`).
  - `lakehouse.bronze.crypto_bars`: 100,650,539 rows (Append-only).
  - `lakehouse.silver.crypto_bars`: 100,650,539 rows (Sanity validated, priority attached).
  - `lakehouse.silver.curated_crypto_bars`: 100,650,539 rows (Deduplicated by business key & priority).
  - `lakehouse.gold.curated_crypto_bars`: 100,650,539 rows (Authoritative gold dataset).
  - `lakehouse.gold.dim_symbol`: 68 symbol mappings (34 Alpaca + 34 Binance).
  - `lakehouse.gold.agg_daily_crypto_ohlcv`: Daily aggregations per exchange.
  - `lakehouse.gold.agg_daily_cross_exchange_spread`: Daily cross-exchange basis & spread.
  - `lakehouse.gold.pipeline_audit`: Tracking batch metrics, row counts, validation results.
- **Serving Analytical Views** (Được định nghĩa trong `sql/ddl/02_create_gold_views.sql`):
  1. `v_active_crypto_symbols`
  2. `v_daily_market_summary`
  3. `v_cross_exchange_daily_spread`
  4. `v_latest_symbol_prices`
  5. `v_fiat_stablecoin_rates` (Trích xuất tỷ giá USDT-USD, USDC-USD phục vụ quy đổi)
  6. `v_cross_exchange_normalized_spread` (Chuẩn hóa giá Binance về USD để tính arbitrage spread không bị méo mó bởi peg)

### Maintained Artifacts
- Các bảng benchmark kiểm thử lưu trong `warehouse/bronze/`: `test_spark_clustered` và `test_spark_range` (Được giữ nguyên để phục vụ tra cứu benchmark).

---

## 5. Phase 2 Roadmap & Next Objectives

1. **Hybrid Cloud Extension**:
   - Đồng bộ hóa dữ liệu từ MinIO local lên AWS S3 (Hybrid Lakehouse).
   - Tích hợp streaming ingestion qua Apache Kafka / AWS MSK.
2. **Dual-Speed Gold Serving**:
   - Phân tách kiến trúc phục vụ: Realtime Gold (sub-second streaming via Redis/DuckDB/ClickHouse) vs Curated Gold (batch authoritative via Iceberg).
3. **Cross-Exchange Arbitrage & ML Feature Store**:
   - Xây dựng pipeline phát hiện cơ hội Arbitrage theo thời gian thực (dựa trên `v_cross_exchange_normalized_spread`).
   - Xây dựng Feature Store tính toán các chỉ báo kỹ thuật (RSI, Bollinger Bands, Volume Weighted Average Price - VWAP) phục vụ mô hình định lượng.

---

## 6. Git Clone Readiness & Secret Governance
- **Bảo mật API & Biến môi trường**:
  - File `.env` chứa mật khẩu thực tế được bảo vệ tuyệt đối bằng `.gitignore` (không bị lộ lên GitHub).
  - File `.env.example` cung cấp đầy đủ các biến mẫu, bao gồm `APCA_API_KEY_ID` và `APCA_API_SECRET_KEY` (cho Alpaca).
  - Binance REST API là public endpoint, chạy trực tiếp mà không cần API key.
- **Tính khả chuyển (Portability)**:
  - Loại bỏ hoàn toàn các đường dẫn tuyệt đối dạng `E:\...` trong mã nguồn Python (`alpaca.py`, `binance.py`). Hệ thống tự động phân giải đường dẫn tương đối qua `__file__`.
  - Mọi câu lệnh trong `README.md` sử dụng đường dẫn Linux nội bộ container (`/home/iceberg/...`) hoặc `./` tại host.
- **Tài liệu song ngữ đơn file**:
  - `README.md` áp dụng cơ chế thẻ `<details open>` cho Tiếng Việt và `<details>` cho Tiếng Anh, cho phép người dùng chuyển đổi ngôn ngữ mượt mà trong cùng một file duy nhất.

