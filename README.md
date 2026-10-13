# Multi-Exchange Crypto Lakehouse Platform
> **Enterprise Data Platform with Apache Iceberg, Apache Spark, MinIO, DuckDB, PyIceberg & Apache Airflow**

---

<p align="center">
  <b>Chuyển đổi ngôn ngữ / Switch Language:</b><br>
  <a href="#-tiếng-việt-hướng-dẫn-khởi-chạy--vận-hành-nhanh"><b>🇻🇳 Tiếng Việt (Khởi chạy nhanh)</b></a> •
  <a href="#-english-architecture--technical-specifications"><b>🇬🇧 English (Technical Specs)</b></a>
</p>

---

<details open>
<summary><h2>🇻🇳 TIẾNG VIỆT: HƯỚNG DẪN KHỞI CHẠY & VẬN HÀNH NHANH</h2></summary>

### 1. Giới thiệu dự án
Nền tảng **Lakehouse tiền mã hóa đa sàn** (Alpaca & Binance) chuẩn Enterprise, tuân thủ nghiêm ngặt mô hình **Medallion Architecture (Bronze $\to$ Silver $\to$ Gold)** trên nền **Apache Iceberg (ACID Tables)** và lưu trữ phân tán **MinIO (S3-compatible)**.

- **Độ tin cậy dữ liệu**: Đã nạp và thẩm định thành công **100,650,539 bản ghi** nến 1 phút (68 cặp giao dịch trên 34 tài sản cơ sở từ 2021 đến 2026).
- **Chất lượng dữ liệu**: **0 khóa trùng lặp**, **0 vi phạm bất đẳng thức OHLCV**, độc lập phân tích liên sàn.

---

### 2. Yêu cầu môi trường (Prerequisites)
Bạn của bạn khi tải project về máy chỉ cần:
1. **Git**
2. **Docker Desktop** (hoặc Docker Engine + Docker Compose v2) đã khởi động.
3. **RAM khuyến nghị**: Tối thiểu 8 GB RAM (cấp ít nhất 4 GB RAM cho Docker).
4. *Không cần cài đặt Spark, Python, Java hay Airflow trên máy thật* — toàn bộ cụm dịch vụ chạy cô lập 100% trong Docker.

---

### 3. Các bước khởi chạy từ Git Clone (5 phút chạy ngay)

#### Bước 1: Clone Repository & Chuẩn bị File Môi trường
```bash
git clone <URL_REPOSITORY_CỦA_BẠN>
cd project_hybrid_lakehouse_crypto

# Tạo file cấu hình môi trường từ mẫu
cp .env.example .env
```

> **Lưu ý về API Key**:
> - **Binance**: Sử dụng Public REST API $\to$ **Hoàn toàn KHÔNG CẦN API Key**, chạy được ngay lập tức!
> - **Alpaca**: Tùy chọn. Nếu muốn lấy thêm dữ liệu nến thời gian thực từ sàn Alpaca, bạn đăng ký tài khoản miễn phí tại [alpaca.markets](https://alpaca.markets/) và điền vào `.env`:
>   ```env
>   APCA_API_KEY_ID=your_key_here
>   APCA_API_SECRET_KEY=your_secret_here
>   ```
>   *(Nếu để trống, pipeline sẽ tự động bỏ qua Alpaca và vẫn nạp dữ liệu Binance bình thường mà không báo lỗi)*.

#### Bước 2: Khởi động toàn bộ Cụm Dịch vụ Lakehouse
```bash
docker compose up -d
```
Đợi khoảng 30–45 giây để các dịch vụ hoàn tất khởi tạo. Kiểm tra trạng thái:
```bash
docker compose ps
```
*(Tất cả 6 container `lakehouse-minio`, `lakehouse-iceberg-rest`, `lakehouse-spark-iceberg`, `lakehouse-postgres`, `lakehouse-airflow-webserver`, `lakehouse-airflow-scheduler` đều ở trạng thái `running` hoặc `healthy`)*.

#### Bước 3: Khởi tạo Cấu trúc Bảng Iceberg & Views Phân tích
Chạy lệnh tạo bảng Medallion và các View nghiệp vụ trực tiếp trong Spark:
```bash
# 1. Tạo các bảng Iceberg (Bronze, Silver, Curated Silver, Gold)
docker exec -i lakehouse-spark-iceberg spark-sql -f /home/iceberg/sql/ddl/01_create_iceberg_tables.sql

# 2. Tạo các Views phân tích kinh doanh chênh lệch giá (Arbitrage) và báo cáo
docker exec -i lakehouse-spark-iceberg spark-sql -f /home/iceberg/sql/ddl/02_create_gold_views.sql
```

#### Bước 4: Chạy Unit Tests kiểm tra môi trường
```bash
docker exec lakehouse-spark-iceberg python3 -m unittest discover -s /home/iceberg/tests -p "test_*.py" -v
```

---

### 4. Kích hoạt Pipeline Dữ liệu

#### Cách A: Chạy Incremental Live Pipeline (Khuyên dùng khi mới tải về)
Không cần tải tệp dữ liệu lịch sử hàng chục GB! Pipeline này sẽ trực tiếp cào các nến 1 phút mới nhất từ sàn về, nạp qua Bronze $\to$ Silver $\to$ Curated Silver $\to$ Gold:

**Kích hoạt bằng CLI:**
```bash
docker exec lakehouse-airflow-webserver airflow dags trigger lakehouse_incremental_hourly
```
**Hoặc kích hoạt trên giao diện Web:**
1. Mở trình duyệt vào `http://localhost:8085` (Tài khoản: `admin` / Mật khẩu: `admin`).
2. Bật công tắc `Unpause` cho DAG **`lakehouse_incremental_hourly`**.
3. Bấm nút **Trigger DAG** (nút Play góc phải).

#### Cách B: Chạy Historical Backfill (Nếu có sẵn dữ liệu thô)
Nếu bạn đã có bộ dữ liệu nến lịch sử từ 2021 đến 2026:
1. Bỏ dữ liệu vào thư mục `./raw/alpaca/bars` và `./raw/binance/kline`.
2. Kích hoạt DAG **`lakehouse_historical_backfill`** trên Airflow UI hoặc qua CLI.

---

### 5. Truy vấn Dữ liệu & Kiểm tra Kết quả
Sau khi pipeline chạy xong, bạn có thể truy vấn ngay từ Spark SQL CLI:

```bash
# 1. Xem 10 bản ghi mới nhất trong Gold Layer
docker exec lakehouse-spark-iceberg spark-sql -e "SELECT exchange, raw_symbol, timestamp, close, volume FROM lakehouse.gold.curated_crypto_bars ORDER BY timestamp DESC LIMIT 10;"

# 2. Xem tỷ giá Stablecoin tham chiếu (USDT-USD, USDC-USD)
docker exec lakehouse-spark-iceberg spark-sql -e "SELECT * FROM lakehouse.gold.v_fiat_stablecoin_rates LIMIT 5;"

# 3. Xem chênh lệch giá giữa Alpaca và Binance đã chuẩn hóa tỷ giá (Daily Normalized Arbitrage - Tức thì < 1s)
docker exec lakehouse-spark-iceberg spark-sql -e "SELECT * FROM lakehouse.gold.v_daily_cross_exchange_normalized_spread LIMIT 10;"

# 4. Xem chênh lệch giá chi tiết từng phút (Bảo vệ tài nguyên với Bounded Serving API, kiểm soát partition pushdown)
docker exec lakehouse-spark-iceberg python src/serving/query_minute_spread.py --base-currency BTC --start-time 2026-08-10T00:00:00Z --end-time 2026-08-11T00:00:00Z
```

---

### 6. Danh sách Cổng Dịch vụ & Giao diện Web (Web UI)

| Giao diện | Địa chỉ truy cập | Tài khoản / Mật khẩu | Chức năng |
| :--- | :--- | :--- | :--- |
| **Airflow Web UI** | [http://localhost:8085](http://localhost:8085) | `admin` / `admin` | Giám sát & kích hoạt các DAG pipeline |
| **MinIO Console (S3)** | [http://localhost:9001](http://localhost:9001) | `admin` / `password` | Quản lý bucket dữ liệu Iceberg `s3://warehouse/` |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) | Không cần | Theo dõi tài nguyên tính toán Spark |
| **Jupyter Notebook** | [http://localhost:8888](http://localhost:8888) | Không cần token | Phân tích dữ liệu & thử nghiệm truy vấn |
| **Iceberg REST API** | [http://localhost:8181](http://localhost:8181) | Internal | Catalog siêu dữ liệu Iceberg |

---

### 7. Những Lưu ý Quan trọng khi Chạy Project
1. **Không dùng đường dẫn tuyệt đối (Absolute Path)**: Mọi câu lệnh trong tài liệu đều dùng đường dẫn tương đối hoặc đường dẫn nội bộ bên trong Docker (`/home/iceberg/...`). Dù bạn clone vào ổ đĩa `C:`, `D:`, hay `E:`, project đều chạy chính xác 100%.
2. **Khóa định danh nghiệp vụ**: Hệ thống sử dụng bộ 3 `(exchange, raw_symbol, timestamp)` làm khóa chính. Dữ liệu giữa Alpaca và Binance độc lập hoàn toàn, không bao giờ ghi đè lẫn nhau.
3. **Bảo toàn đồng tiền gốc**: Giá ở bảng cơ sở giữ nguyên đơn vị tiền tệ gốc (`USD` cho Alpaca, `USDT`/`USDC` cho Binance). Quy đổi tỷ giá chỉ diễn ra tại tầng Serving View.

</details>

---

<details>
<summary><h2>🇬🇧 ENGLISH: ARCHITECTURE & TECHNICAL SPECIFICATIONS</h2></summary>

### 1. Core Architecture Principles
1. **Multi-Exchange Independence**:
   - Alpaca and Binance operate separate order books, liquidity pools, and matching engines.
   - Records for the same underlying asset at the same minute (e.g., `BTC/USD` on Alpaca vs `BTCUSDT` on Binance) are distinct market observations and **NEVER overwrite each other**.
2. **Expanded Business Key**:
   - The canonical primary key across the entire lakehouse is:
     $$\text{Business Key} = (\mathbf{exchange}, \mathbf{raw\_symbol}, \mathbf{timestamp})$$
3. **Intra-Exchange Priority Conflict Resolution**:
   - Priority comparison ($\mathbf{3 > 2 > 1}$) applies strictly **within the same exchange**:
     - **Priority 3**: High-audit historical backfill or exchange correction.
     - **Priority 2**: Scheduled REST API hourly batch.
     - **Priority 1**: Realtime WebSocket stream (preliminary bar).
4. **No Synthetic / Artificial Rows**:
   - Assets begin strictly at their real exchange listing date ($\ge 2021-01-01$ UTC). No synthetic forward-fill for unlisted periods.
5. **Quote Currency Preservation**:
   - Native quote currency (`USD`, `USDT`, `USDC`) is preserved in base physical tables. Normalization occurs exclusively at the analytical serving layer view (`v_daily_cross_exchange_normalized_spread`) or via bounded serving query API (`src/serving/query_minute_spread.py`).

---

### 2. Medallion Layer Architecture

```
 ┌───────────────────────────┐         ┌───────────────────────────┐
 │   Raw Alpaca Market Data  │         │   Raw Binance Market Data │
 │   (raw/alpaca/bars/)      │         │   (raw/binance/kline/)    │
 └─────────────┬─────────────┘         └─────────────┬─────────────┘
               │                                     │
               ▼                                     ▼
 ┌───────────────────────────┐         ┌───────────────────────────┐
 │ src/preprocessing/        │         │ src/preprocessing/        │
 │ alpaca.py                 │         │ binance.py                │
 └─────────────┬─────────────┘         └─────────────┬─────────────┘
               │                                     │
               └──────────────────┬──────────────────┘
                                  │
                                  ▼
                   ┌───────────────────────────────┐
                   │   Canonical Staging Parquet   │
                   │ (warehouse/staging/canonical) │
                   └──────────────┬────────────────┘
                                  │
                                  ▼
                   ┌───────────────────────────────┐
                   │    Iceberg Bronze Layer       │
                   │ (lakehouse.bronze.crypto_bars)│
                   │  PARTITIONED BY (days(ts))    │
                   └──────────────┬────────────────┘
                                  │
                                  ▼
                   ┌───────────────────────────────┐
                   │    Iceberg Silver Layer       │
                   │ (lakehouse.silver.crypto_bars)│
                   │ Cleansed, Sanity, Priority=2  │
                   └──────────────┬────────────────┘
                                  │
                                  ▼
                   ┌───────────────────────────────────────┐
                   │     Iceberg Curated Silver            │
                   │ (lakehouse.silver.curated_crypto_bars)│
                   │ MERGE ON (exchange, raw_symbol, ts)   │
                   │ Intra-exchange Priority (3 > 2 > 1)   │
                   └──────────────┬────────────────────────┘
                                  │
                                  ▼
                   ┌───────────────────────────────────────┐
                   │      Iceberg Curated Gold             │
                   │ - lakehouse.gold.curated_crypto_bars  │
                   │   (Authoritative 1-min Fact Table)    │
                   │ - lakehouse.gold.dim_symbol           │
                   │ - lakehouse.gold.agg_daily_crypto_ohlcv│
                   │ - gold.agg_daily_cross_exchange_spread│
                   └──────────────┬────────────────────────┘
                                  │
                   ┌──────────────┴──────────────┐
                   ▼                             ▼
       Gold Analytical Views             Pipeline Audit & DQ
       - v_daily_market_summary          (lakehouse.gold.pipeline_audit)
       - v_daily_cross_exchange_normalized_spread
       - src/serving/query_minute_spread.py
       - v_fiat_stablecoin_rates
```

---

### 3. Production Verification Summary (101M Records Backfill)

| Medallion Layer | Iceberg Table | Record Count | Processing Speed | Status |
| :--- | :--- | :---: | :---: | :---: |
| **Bronze** | `lakehouse.bronze.crypto_bars` | 101,053,358 | 780.6s (129,456 rows/s) | `SUCCESS` |
| **Silver** | `lakehouse.silver.crypto_bars` | 100,869,347 | 1,421.2s (70,974 rows/s) | `SUCCESS` |
| **Curated Silver** | `lakehouse.silver.curated_crypto_bars` | 100,747,585 | 1,415.1s (71,194 rows/s) | `SUCCESS` |
| **Curated Gold** | `lakehouse.gold.curated_crypto_bars` | 100,650,539 | 1,338.6s (75,188 rows/s) | `SUCCESS` |
| **Data Quality** | Duplicate Keys = 0, Sanity Violations = 0 | 100,650,539 | Verified | `SUCCESS` |

---

### 4. Zero-JVM-OOM Architecture (PyIceberg + DuckDB)
- **Root Cause Analysis**: Writing 101M rows across 1,584 partition days via Spark's `PartitionedFanoutWriter` provoked severe JVM heap exhaustion ($1,584 \times 64\text{MB} \approx 101\text{GB}$ buffer demand).
- **Optimization Strategy**: Replaced wide batch ingestion writes with streaming **PyIceberg** (C++ native write memory) and vectorized in-process **DuckDB** window deduplication (`QUALIFY ROW_NUMBER() = 1`), reducing peak memory to $< 2.8\text{ GB}$ RAM while sustaining $71,000 - 129,000\text{ rows/s}$.

---

### 5. Modular Documentation Tree
Comprehensive technical documentation is organized within [`docs/`](docs/):
- **Architecture & Storage**: [`docs/1-architecture/storage-and-warehouse.md`](docs/1-architecture/storage-and-warehouse.md)
- **Postmortems & Benchmarks**: [`docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md`](docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md)
- **Phase 1 Specification**: [`docs/3-specifications/phase1-migration-spec.md`](docs/3-specifications/phase1-migration-spec.md)
- **Operations & Runbooks**: [`docs/4-runbooks/operations-and-maintenance.md`](docs/4-runbooks/operations-and-maintenance.md)
- **Data Contracts**: [`docs/5-data-contracts/symbol-and-currency-contract.md`](docs/5-data-contracts/symbol-and-currency-contract.md)
- **Project Memory**: [`MEMORY.md`](MEMORY.md)
- **Agent Operational Rules**: [`.agent/rules/`](.agent/rules/)

</details>
