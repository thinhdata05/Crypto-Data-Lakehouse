# Multi-Exchange Crypto Lakehouse Platform (`s3://lakehouse/`)

Nền tảng **Data Lakehouse Đa Sàn Giao Dịch Tiền Mã Hóa** chuẩn kiến trúc Senior Data Engineer, được xây dựng trên **Apache Iceberg**, **PyIceberg**, **DuckDB**, **Apache Spark SQL**, **MinIO (`s3://lakehouse/`)**, và **Apache Airflow**.

Hệ thống thu thập, chuẩn hóa, kiểm định chất lượng và hợp nhất hơn **104.7 triệu bản ghi nến 1 phút (1-minute OHLCV bars)** từ **Alpaca Crypto** (34 symbols) và **Binance** (34 symbols) giai đoạn `2021–2026`, đồng thời sẵn sàng cho kiến trúc **Phase 2 Hybrid Cloud (2 Medallion song song hội tụ tại Cloud Silver `silver.bars`)**.

---

## 1. Các Điểm Nhấn Kiến Trúc Cốt Lõi (Textbook Senior DE Architecture)

1. **Bucket Chuẩn & Phân Tách Control Plane / Data Plane Trên `s3://lakehouse/`**:
   - **Control Plane (`lakehouse.ops`)**: Tách biệt hoàn toàn dữ liệu quản trị vận hành (`ops.ingestion_manifest` lưu mã băm SHA-256 cấp độ file và `ops.pipeline_audit` lưu telemetry thực thi) khỏi các tầng dữ liệu nghiệp vụ.
   - **Local Data Plane (`s3://lakehouse/`)**:
     - `lakehouse.bronze.crypto_bars`: **`104,706,818` dòng** bất biến (Append-Only) trên **`2,071` file Parquet** phân vùng theo ngày (`days(timestamp)`).
     - `lakehouse.silver.bars`: **`104,657,836` dòng** chuẩn hóa duy nhất trên **`2,058` file Parquet** (đã lọc bỏ `48,982` dòng trùng lặp/vi phạm 7 quy tắc OHLCV, gán trọng số ưu tiên động $3 > 2 > 1$, và khử trùng lặp 100% theo khóa `(exchange, raw_symbol, timestamp)`).
2. **Độc Lập Đa Sàn & Giải Quyết Xung Đột Nội Bộ Sàn ($3 > 2 > 1$)**:
   - Khóa định danh hợp nhất: $\text{Business Key} = (\text{exchange}, \text{raw\_symbol}, \text{timestamp})$.
   - Dữ liệu giữa Alpaca và Binance không bao giờ ghi đè lên nhau. Quy tắc ưu tiên (`3`: WebSocket correction `T='u'` > `2`: Historical & Incremental REST > `1`: Initial WebSocket `T='b'`, phân xử hòa bằng `ingested_at DESC`) chỉ áp dụng trong cùng một sàn.
3. **Zero-JVM-OOM & Triệt Tiêu Small-File Explosion (Giảm 41 Lần Số File)**:
   - Toàn bộ luồng ETL chạy bằng **PyIceberg + DuckDB In-Process Vectorization** kết hợp gom nhóm theo Quý (`raw_to_bronze.py`) và theo Tháng (`bronze_to_silver.py`) trên toàn bộ 68 symbols.
   - Giảm số lượng file Parquet từ `84,149` file/bảng xuống còn **`~2,058` file/bảng** (1 file hợp nhất cho mỗi ngày UTC trong từng batch window), tốc độ đạt **`986,372` dòng/giây** với mức RAM đỉnh `< 800 MB` (số liệu thực nghiệm đo đạc trên máy trạm dev).
4. **Kiến Trúc Phase 2 Hybrid (Option 1 — Local Lightweight Producer & Single Cloud Gold)**:
   - Local dừng tại **`lakehouse.silver.bars`** (không tạo bảng Gold ở Local để chống hiện tượng Split-Brain) và xuất bản lát cắt Parquet bất biến + atomic `_manifest.json` hàng giờ ra `staging/confirmed/` (hoặc upload thẳng lên Cloud S3 và xác minh checksum nếu được cấu hình).
   - Toàn bộ thiết kế tầng Cloud được quy hoạch thành **bản đặc tả tham khảo duy nhất** tại [`docs/3-specifications/phase2-cloud-setup-spec.md`](docs/3-specifications/phase2-cloud-setup-spec.md). Việc Cloud nạp các file Parquet này vào Iceberg Silver và vật lý hóa 7 Views Gold là tiến trình độc lập thuộc hạ tầng Cloud.

---

## 2. Sơ Đồ Luồng Dữ Liệu Tổng Thể (End-to-End Pipeline Flow)

```text
[NGUỒN DỮ LIỆU THÔ LOCAL]
./raw/alpaca/bars (38,677 file ngày)  ─┐
./raw/binance/bars (49,016 file ngày) ─┴─► Bước 1: Pre-Bronze Coalescing (alpaca.py & binance.py)
                                           - Lọc giữ 68 symbols thanh khoản chính (USDT > USDC > USD > BTC)
                                           - Đồng nhất kiểu int64/double (permissive schema promotion)
                                           - Gom 87,693 file ngày thành 2,939 file tháng tại ./staging/canonical/
                                                               │
                                                               ▼
[KHO ICEBERG LOCAL: s3://lakehouse/]       Bước 2: Partition-Aligned Bronze (raw_to_bronze.py)
├── ops.ingestion_manifest (3,007 SHA-256) - Kiểm tra SHA-256 chống nạp trùng file
├── ops.pipeline_audit                     - Gom 68 symbols theo Quý (YYYY-Q1..Q4) -> 2,071 file Parquet ngày
└── bronze.crypto_bars (104,706,818 dòng)                      │
                                                               ▼
                                           Bước 3: Vectorized Silver (bronze_to_silver.py)
                                           - Streaming theo Tháng (YYYY-MM) qua DuckDB (< 800 MB RAM)
                                           - Lọc 7 quy tắc OHLCV + Gán priority (3 > 2 > 1) + Deduplicate
                                                               │
                                                               ▼
└── silver.bars (104,657,836 dòng, 2,058 file) ◄── Bước 4: Hourly Incremental DAG (lakehouse_incremental_hourly)
          │                                        - rest_fetcher.py -> raw_to_bronze.py -> incremental_sync.py
          ▼
./staging/confirmed/batch_id=<uuid>/
  ├── alpaca_bars.parquet (priority=2)     ─┐
  ├── binance_bars.parquet (priority=2)    ─┼─► Bước 5 (Kiến trúc Cloud đề xuất): Single-Writer Convergence Service (EMR Serverless / Glue / ECS)
  └── _manifest.json (SHA-256 commit)      ─┘   - Dự kiến hợp nhất với bronze.rt_bars (WS Alpaca + WS Binance, priority=1,3)
                                                - Dự kiến cập nhật Cloud silver.bars -> Materialize Cloud gold.*
```

---

## 3. Bảng Số Liệu Thực Nghiệm & Benchmark (`s3://lakehouse/`)

| Hạng Mục / Pipeline Step | Số Lượng Bản Ghi | Số File Parquet | Thời Gian Chạy | Thông Lượng / RAM Đỉnh |
| :--- | :--- | :---: | :---: | :--- |
| **Pre-Bronze (`alpaca.py` + `binance.py`)** | `104,706,818` dòng (`0` lỗi) | `2,939` file tháng | `~6.5 phút` | Đa nhân CPU Host (`promote_options="permissive"`) |
| **Bronze (`lakehouse.bronze.crypto_bars`)** | **`104,706,818` dòng** | **`2,071` file ngày** | `~3.2 phút` | Gom theo Quý (`YYYY-Q1..Q4`), Peak RAM `< 1.2 GB` |
| **Silver (`lakehouse.silver.bars`)** | **`104,657,836` dòng** | **`2,058` file ngày** | **`106.1 giây`** | **`986,372` rows/s**, Peak RAM **`< 800 MB`** |
| **Quality Audit (`data_validation.py`)** | Kiểm định `104.66M` dòng | Quét 27 Quý | **`22.7 giây`** | `0` duplicates, `0` OHLCV violations, RAM `< 500 MB` |
| **Airflow Hourly DAG (`lakehouse_incremental_hourly`)** | `240` dòng REST/lượt | Xuất 2 file `confirmed/` | **`34.0 giây`** | Hoàn tất 6/6 tasks (`SUCCESS`) |
| **Bộ Kiểm Thử Tự Động (`tests/`)** | **25/25 Tests (`OK`)** | — | **`~3.0 giây`** | Bao gồm Contract, S3 Upload, Atomic Manifest, & Live Convergence |

> *Ghi chú benchmark*: Các chỉ số thông lượng (~986k dòng/s) và RAM (< 800 MB) là kết quả đo thực nghiệm trên tập dữ liệu 104M dòng trên máy trạm dev. Quy tắc 1 file/ngày đạt được trong mỗi lượt gom cửa sổ batch; các đợt nạp micro-batch nhỏ sẽ tích lũy file cho đến khi chạy lệnh Iceberg compaction.

---

## 4. Hướng Dẫn Khởi Động Nhanh (Quickstart)

### 4.1. Khởi Động Hạ Tầng Docker
```bash
cp .env.example .env
docker compose up -d
```

### 4.2. Chạy Pipeline Nạp Lịch Sử (Historical Backfill — CLI)
```bash
# 1. Tạo bảng Iceberg trên s3://lakehouse/
docker exec -w /home/iceberg lakehouse-spark-iceberg spark-sql -f /home/iceberg/sql/ddl/01_create_iceberg_tables.sql

# 2. Nạp từ ./staging/canonical vào bronze.crypto_bars (tự động SKIP các file đã có SHA-256 trong ops.ingestion_manifest)
docker exec -w /home/iceberg lakehouse-spark-iceberg python3 src/pipeline/bronze/raw_to_bronze.py --batch-id manual_run

# 3. Làm sạch & khử trùng lặp vào silver.bars (tự động SKIP các tháng đã nạp)
docker exec -w /home/iceberg lakehouse-spark-iceberg python3 src/pipeline/silver/bronze_to_silver.py --batch-id manual_run --target-table silver.bars

# 4. Kiểm định chất lượng dữ liệu trên silver.bars
docker exec -w /home/iceberg lakehouse-spark-iceberg python3 src/pipeline/quality/data_validation.py --run-id manual_run --target-table silver.bars
```

### 4.3. Chạy Kiểm Thử Airflow Incremental DAG & Unit Tests
```bash
# Chạy toàn bộ 25 Unit & Integration Tests:
docker exec -w /home/iceberg lakehouse-spark-iceberg python3 -m unittest discover -s tests -v

# Chạy kiểm thử toàn trình DAG Incremental Hourly trên Airflow:
docker exec lakehouse-airflow-scheduler airflow dags test lakehouse_incremental_hourly 2026-11-08T07:00:00Z
```

---

## 5. Danh Mục Tài Liệu Chi Tiết (`docs/`)

- **Kiến trúc & Lưu trữ**: [`docs/1-architecture/storage-and-warehouse.md`](docs/1-architecture/storage-and-warehouse.md)
- **Post-Mortems, RCA & Benchmarks**: [`docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md`](docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md)
- **Đặc tả Kỹ thuật Local Medallion**: [`docs/3-specifications/phase1-migration-spec.md`](docs/3-specifications/phase1-migration-spec.md)
- **Đặc tả Thiết lập Cloud Phase 2**: [`docs/3-specifications/phase2-cloud-setup-spec.md`](docs/3-specifications/phase2-cloud-setup-spec.md)
- **Runbook Kiểm toán Partition Spec**: [`docs/4-runbooks/catalog-partition-migration.md`](docs/4-runbooks/catalog-partition-migration.md)
- **Runbook Vận hành & Bảo trì VHDX**: [`docs/4-runbooks/operations-and-maintenance.md`](docs/4-runbooks/operations-and-maintenance.md)
- **Data Contract & Vi mô Thị trường**: [`docs/5-data-contracts/symbol-and-currency-contract.md`](docs/5-data-contracts/symbol-and-currency-contract.md)
