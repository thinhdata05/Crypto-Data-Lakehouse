# Đặc Tả Kỹ Thuật Nền Tảng: Multi-Exchange Crypto Lakehouse (Textbook Senior DE Architecture)

> **Tài liệu đặc tả kỹ thuật chính thức**: Định nghĩa toàn bộ kiến trúc dữ liệu, quy tắc nghiệp vụ đa sàn, mô hình phân vùng, cơ chế nạp dữ liệu không tràn bộ nhớ (Zero-JVM-OOM) và hệ thống kiểm định chất lượng của dự án **Multi-Exchange Crypto Lakehouse Platform**.

---

## 1. Mục Tiêu & Phạm Vi Hệ Thống

Xây dựng nền tảng Data Lakehouse chuẩn sản xuất trên **Apache Iceberg (`s3://lakehouse/`)**, xử lý và hợp nhất dữ liệu nến 1 phút (1-minute OHLCV bars) từ hai sàn giao dịch tiền mã hóa lớn có cấu trúc thị trường khác biệt:
1. **Alpaca Crypto**: Định giá chủ đạo bằng tiền pháp định (`USD`), đại diện thanh khoản thị trường Hoa Kỳ (34 cặp giao dịch).
2. **Binance**: Định giá chủ đạo bằng Stablecoin (`USDT`, `USDC`), đại diện bể thanh khoản toàn cầu (34 cặp giao dịch).

### Các Chỉ Số Đã Kiểm Chứng Thực Tế Trên Kho `s3://lakehouse/`
- **Tổng số bản ghi tầng Bronze (`lakehouse.bronze.crypto_bars`)**: **`104,706,818` dòng** trên **2,071 file Parquet** (`0` lỗi tiền xử lý).
- **Tổng số bản ghi tầng Silver (`lakehouse.silver.bars`)**: **`104,657,836` dòng** trên **2,058 file Parquet** (đã lọc bỏ `48,982` dòng trùng lặp hoặc vi phạm 7 quy tắc OHLCV).
- **Tổng số bản ghi Control Plane (`lakehouse.ops.ingestion_manifest`)**: **`3,007` bản ghi** kiểm soát mã băm SHA-256 cấp độ file.
- **Độ phủ tài sản**: **68 cặp giao dịch** (34 Alpaca + 34 Binance) trải dài từ `2021-01-01 UTC` đến `2026-08 UTC`.
- **Kiểm thử tự động**: **24/24 Unit & Live Integration Tests** đạt `PASS 100%`.

---

## 2. Cấu Trúc Thư Mục Mã Nguồn Chuẩn Hóa

```text
project_hybrid_lakehouse_crypto/
├── AGENTS.md                                 # Hướng dẫn vận hành cho AI Agent (Tiếng Anh)
├── MEMORY.md                                 # Trạng thái hiện hành của Lakehouse cho AI Agent (Tiếng Anh)
├── README.md                                 # Tài liệu tổng quan & hướng dẫn chạy nhanh (Tiếng Việt)
├── docker-compose.yml                        # Hạ tầng 6 containers (MinIO, REST Catalog, Spark, Postgres, Airflow)
├── .agent/rules/                             # Bộ 4 quy tắc bất biến dành cho AI Agent (Tiếng Anh)
├── airflow/dags/
│   ├── lakehouse_historical_backfill_dag.py  # DAG nạp lịch sử (Raw -> Bronze -> Silver -> Validation)
│   └── lakehouse_incremental_hourly_dag.py   # DAG nạp hàng giờ (REST -> Bronze -> Silver -> Confirmed Parquet)
├── sql/ddl/
│   └── 01_create_iceberg_tables.sql          # DDL khởi tạo bảng Iceberg Local (ops.*, bronze.crypto_bars, silver.bars)
├── src/
│   ├── preprocessing/
│   │   ├── normalizer.py                     # Chuẩn hóa ký hiệu, thứ tự ưu tiên (3 > 2 > 1) & kiểm tra OHLCV
│   │   ├── alpaca.py                         # Tiền xử lý & gom file tháng cho Alpaca (permissive promotion)
│   │   └── binance.py                        # Tiền xử lý & gom file tháng cho Binance (lọc USDT > USDC > USD > BTC)
│   ├── pipeline/
│   │   ├── common/iceberg_session.py         # Khởi tạo kết nối Spark / Iceberg Catalog (s3://lakehouse/)
│   │   ├── bronze/raw_to_bronze.py           # Nạp Bronze gom theo Quý (YYYY-Q1..Q4) + kiểm tra SHA-256
│   │   ├── silver/bronze_to_silver.py        # Làm sạch & khử trùng lặp vào silver.bars theo Tháng (YYYY-MM)
│   │   ├── incremental/
│   │   │   ├── rest_fetcher.py               # Kéo nến 1 phút mới nhất từ REST API của Binance & Alpaca
│   │   │   ├── incremental_sync.py           # Hợp nhất cửa sổ ngày vào silver.bars bằng DuckDB + PyIceberg
│   │   │   └── publish_confirmed_parquet.py  # Xuất Parquet bất biến + atomic _manifest.json ra confirmed/
│   │   └── quality/data_validation.py        # Kiểm định chất lượng theo Quý trên silver.bars
│   ├── serving/query_minute_spread.py        # API truy vấn spread cấp phút có khóa bảo vệ 7 ngày
│   └── utils/
│       ├── manifest_tracker.py               # Quản lý SHA-256 lineage trên ops.ingestion_manifest
│       └── verify_and_migrate_catalog.py     # Kiểm toán và tiến hóa Partition Spec tự động
└── tests/                                    # Bộ kiểm thử hợp đồng dữ liệu & tính lũy kế (25 tests)
```

---

## 3. Đặc Tả Chi Tiết Các Tầng Dữ Liệu (Control Plane & Data Plane)

### 3.1. Control Plane (`lakehouse.ops`)
1. **`lakehouse.ops.ingestion_manifest`** (`PARTITIONED BY (days(processed_at))`):
   - Lưu trữ `exchange`, `raw_symbol`, `canonical_symbol`, `source_file`, `file_hash` (SHA-256), `file_size_bytes`, `record_count`, `min_timestamp`, `max_timestamp`, `batch_id`, `status`, `processed_at`.
   - Đóng vai trò cổng kiểm soát lũy kế (Idempotency Gate) cho tầng Bronze.
2. **`lakehouse.ops.pipeline_audit`** (`PARTITIONED BY (days(created_at))`):
   - Lưu trữ `run_id`, `dag_id`, `step_name`, `status`, `records_processed`, `details`, `created_at`.

### 3.2. Local Data Plane (`s3://lakehouse/`)
1. **`lakehouse.bronze.crypto_bars`** (`PARTITIONED BY (days(timestamp))`):
   - Lưu trữ bất biến (Append-only) toàn bộ dữ liệu nến chuẩn hóa từ `./staging/canonical`.
2. **`lakehouse.silver.bars`** (`PARTITIONED BY (days(timestamp))`):
   - Bảng chuẩn duy nhất tại Local: lọc 7 quy tắc OHLCV (kèm trade_count >= 0), gán `priority` động ($3 > 2 > 1$), và khử trùng lặp theo `(exchange, raw_symbol, timestamp)` (`ORDER BY priority DESC, ingested_at DESC`).

### 3.3. Cloud Data Plane Proposal (`s3://<cloud>-lakehouse/`)
*(Đặc tả đề xuất kiến trúc Phase 2 tại [`docs/3-specifications/phase2-cloud-setup-spec.md`](phase2-cloud-setup-spec.md))*:
1. **`lakehouse.bronze.rt_bars`** (`PARTITIONED BY (days(timestamp))`):
   - Lưu trữ luồng WebSocket thời gian thực từ cả Alpaca và Binance (`priority = 1` cho nến đóng lần đầu `T='b'`, `priority = 3` cho nến đính chính `T='u'`).
2. **`lakehouse.silver.bars`** (`PARTITIONED BY (days(timestamp))`):
   - Hội tụ Single Writer hợp nhất `bronze.rt_bars` và `confirmed/`.
3. **`lakehouse.gold.*`** (Single Source of Truth duy nhất trên Cloud):
   - `gold.dim_symbol`, `gold.dim_date`, `gold.agg_daily_crypto_ohlcv`, `gold.agg_daily_cross_exchange_spread`, và 7 Views phân tích được đặc tả chi tiết tại Mục 5 của [`phase2-cloud-setup-spec.md`](phase2-cloud-setup-spec.md) (`v_daily_crypto_ohlcv`, `v_hourly_crypto_ohlcv`, `v_cross_exchange_daily_spread`, `v_cross_exchange_comparison`, `v_pipeline_audit_summary`, `v_fiat_stablecoin_rates`, `v_daily_cross_exchange_normalized_spread`).
