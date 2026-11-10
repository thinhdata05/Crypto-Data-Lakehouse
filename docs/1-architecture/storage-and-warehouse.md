# Kiến Trúc Lưu Trữ Lakehouse (`s3://lakehouse/`), Phân Tách Control Plane & Hội Tụ Cloud Silver

Tài liệu này định nghĩa kiến trúc lưu trữ chuẩn của dự án **Multi-Exchange Crypto Lakehouse Platform**: lý do sử dụng bucket `s3://lakehouse/`, sự phân tách giữa **Control Plane (`lakehouse.ops`)** và **Data Plane (`bronze`, `silver`, `gold`)**, vai trò thực sự của tầng đệm local `./staging`, và kiến trúc **Phase 2 Hybrid (Option 1 — 2 Medallion song song hội tụ tại Cloud Silver `silver.bars`)**.

---

## 1. Tại Sao Lưu Trữ Trong Bucket `s3://lakehouse/`?

Trong cấu hình của Apache Iceberg và Spark (`spark.sql.catalog.lakehouse.warehouse`), từ khóa `warehouse` chỉ là **tên tham số cấu hình (config key)** chỉ định đường dẫn gốc của Catalog, hoàn toàn không bắt buộc tên bucket vật lý phải là `warehouse`.

Dự án đặt tên bucket vật lý trên MinIO S3 là **`s3://lakehouse/`** (với DNS alias `lakehouse.minio` trên mạng `lakehouse-net`) vì 3 lý do cốt lõi:
1. **Đúng bản chất kiến trúc**: Hệ thống lưu trữ dữ liệu mở (Open Table Format Apache Iceberg trên Parquet) kết hợp khả năng ACID của Data Warehouse và tính linh hoạt của Data Lake.
2. **Tách biệt rõ khỏi thư mục local**: Ngăn chặn mọi nhầm lẫn giữa thư mục đệm trên ổ cứng máy chủ (`./staging`) và kho lưu trữ đối tượng thực sự trên MinIO (`s3://lakehouse/`).
3. **Đồng bộ tên Catalog và Bucket**: Tên Catalog (`lakehouse`) trùng khớp trực tiếp với URI lưu trữ (`s3://lakehouse/`).

---

## 2. Phân Tách Control Plane (`lakehouse.ops`) & Data Plane (`bronze`, `silver`, `gold`)

Theo chuẩn kiến trúc Data Engineering hiện đại, các bảng quản trị vận hành (Operational Metadata, Lineage, Audit) **không được đặt lẫn lộn** vào trong các namespace dữ liệu nghiệp vụ (`bronze`, `silver`, `gold`). Hệ thống phân tách thành 2 mặt phẳng độc lập trên `s3://lakehouse/`:

```text
s3://lakehouse/
├── ops/                             # CONTROL PLANE (Quản trị vận hành & Kiểm toán)
│   ├── ingestion_manifest/          # Kiểm soát lũy kế file-level SHA-256 (days(processed_at))
│   └── pipeline_audit/              # Telemetry & lịch sử thực thi pipeline (days(created_at))
│
├── bronze/                          # DATA PLANE — Tầng 1: Bất biến (Append-Only)
│   ├── crypto_bars/                 # [Local] Dữ liệu lịch sử & REST API (104,706,818 dòng, 2,071 file)
│   └── rt_bars/                     # [Cloud] Dữ liệu WebSocket thời gian thực (WS Alpaca + WS Binance)
│
├── silver/                          # DATA PLANE — Tầng 2: Làm sạch, Gán Priority & Khử trùng lặp
│   └── bars/                        # Bảng Silver chuẩn duy nhất (104,657,836 dòng, 2,058 file)
│
└── gold/                            # DATA PLANE — Tầng 3: Phục vụ Phân tích (Duy nhất trên Cloud)
    ├── dim_symbol/                  # Chiều danh mục 68 symbols (SCD Type 1)
    ├── dim_date/                    # Chiều thời gian lịch chuẩn
    ├── agg_daily_crypto_ohlcv/      # Tổng hợp OHLCV & VWAP theo ngày (days(trade_date))
    └── agg_daily_cross_exchange_spread/ # Tổng hợp chênh lệch giá liên sàn theo ngày
```

### Vì Sao Chỉ Duy Trì Một Bảng `silver.bars` Duy Nhất (Bỏ Thiết Kế Tách Đôi Silver)?
- Ở thiết kế sơ khai, tầng Silver bị tách thành 2 bảng vật lý (`silver.crypto_bars` và `silver.curated_crypto_bars`), dẫn đến việc phải ghi và lưu trữ trùng lặp 104.7 triệu dòng hai lần trên đĩa mà không mang lại giá trị nghiệp vụ mới.
- Trong kiến trúc chuẩn hiện tại, cả Local và Cloud đều thống nhất một bảng **`lakehouse.silver.bars`** duy nhất:
  - Thực hiện trọn vẹn trong **1 bước вектор hóa (Single-Pass DuckDB Vectorization)**:
    1. Kiểm tra 7 quy tắc toán học OHLCV (`OHLCV_SANITY_SQL_PREDICATE`).
    2. Gán trọng số ưu tiên động `priority` ($3 > 2 > 1$).
    3. Khử trùng lặp tuyệt đối theo khóa chính `(exchange, raw_symbol, timestamp)` với thứ tự ưu tiên `ORDER BY priority DESC, ingested_at DESC`.

---

## 3. Phân Định Rõ Vai Trò Giữa `./raw`, `./staging` Và `s3://lakehouse/`

| Thành Phần | Vị Trí Vật Lý | Bản Chất & Nhiệm Vụ | Lưu Trữ Lâu Dài? |
| :--- | :--- | :--- | :---: |
| **`./raw/`** | Ổ cứng Host (`E:\...\raw\`) | Nguồn dữ liệu thô ban đầu: **87,693 file Parquet nhỏ lẻ theo ngày** (`38,677` file Alpaca + `49,016` file Binance). | Có (Read-Only Backup) |
| **`./staging/canonical/`** | Ổ cứng Host (`E:\...\staging\canonical\`) | **Tầng đệm Pre-Bronze (Coalescing Buffer)**: Gom 87,693 file ngày thành **2,939 file tháng**, đồng nhất kiểu dữ liệu `int64`/`double` (`promote_options="permissive"`), và lọc giữ 68 cặp thanh khoản chính. | Không (Có thể xóa sau khi nạp Bronze) |
| **`./staging/confirmed/`** | Ổ cứng Host (`E:\...\staging\confirmed\`) | **Tầng đệm Outbox cho Cloud**: Chứa các lát cắt Parquet hàng giờ (`alpaca_bars.parquet`, `binance_bars.parquet`) + `_manifest.json` để đẩy lên Cloud S3 `confirmed/`. | Không (TTL 7 ngày) |
| **`s3://lakehouse/`** | MinIO Volume (`docker_data.vhdx`) | **Kho Data Lakehouse chính thức**: Chứa toàn bộ bảng Iceberg (`ops.*`, `bronze.crypto_bars`, `silver.bars`) kèm metadata, snapshots và manifest lists. | **CÓ (Authoritative Storage)** |

---

## 4. Giải Thích Chi Tiết Luồng Nạp Dữ Liệu Hiện Tại (Data Ingestion Flow)

### A. Câu Hỏi Cốt Lõi: Tại Sao Không Đẩy Thẳng 87,693 File `./raw` Vào `bronze.crypto_bars` Mà Đi Qua `./staging/canonical`?
1. **Vấn đề kỹ thuật của `./raw`**:
   - Thư mục `./raw` chứa **87,693 file Parquet rời rạc theo từng ngày** (mỗi file chỉ vài KB đến vài chục KB). Nếu mở giao dịch Iceberg append trực tiếp 87,693 lần hoặc đọc từng file qua S3 API, hệ thống sẽ bị nghẽn I/O nghiêm trọng.
   - Ngoài ra, dữ liệu thô của Alpaca có hiện tượng **trôi kiểu dữ liệu (Schema Drift)**: những ngày không có giao dịch thì cột `volume` có kiểu `int64` (`0`), còn những ngày có giao dịch lại mang kiểu `double`, gây lỗi `ArrowInvalid: Unable to merge: Field volume has incompatible types: double vs int64` trên 123 tháng dữ liệu.
2. **Cách hoạt động hiện tại của Preprocessor (`alpaca.py` & `binance.py`)**:
   - Đọc song song đa tiến trình (`ProcessPoolExecutor`), dùng `pa.concat_tables(tables, promote_options="permissive")` để tự động nâng cấp `int64 -> double` (cứu toàn bộ **123 tháng = 3,653,460 dòng** của Alpaca), lọc giữ 68 symbols chuẩn theo thứ tự thanh khoản (`USDT > USDC > USD > BTC`), và gom thành **2,939 file Parquet theo tháng** tại `./staging/canonical/`.
   - **Lưu ý trung thực về mặt lý thuyết Medallion**: Hiện tại trong `alpaca.py` và `binance.py` có thực hiện lọc sơ bộ `timestamp >= 2021-01-01` và `open > 0 & high >= low & volume >= 0` ngay khi tạo file canonical. Do đó, dữ liệu khi vào `bronze.crypto_bars` (`104,706,818` dòng) là **Canonical Raw Bars** (đã chuẩn hóa cấu trúc cột và lọc sơ bộ nến rác âm/trước 2021), chứ chưa phải là bản dump thô nguyên xi 100% của các cặp tiền phụ.

### B. Luồng Nạp Từ `./staging/canonical` Vào `lakehouse.bronze.crypto_bars` (`raw_to_bronze.py`)
1. **Kiểm tra lũy kế cấp độ file (Per-File SHA-256 Gate)**:
   - Tải danh sách mã băm SHA-256 từ `lakehouse.ops.ingestion_manifest`. Mọi file `.parquet` đã từng nạp sẽ được bỏ qua tức thì (`SKIP`).
2. **Gom nhóm theo Quý căn chỉnh phân vùng (Partition-Aligned Quarter Batching `YYYY-Q1..Q4`)**:
   - Thay vì lặp qua từng symbol ở vòng lặp ngoài (gây ra $68 \times 1,300 = 84,149$ file nhỏ làm phình `docker_data.vhdx` lên 86 GB như trước đây), `raw_to_bronze.py` gom **toàn bộ 68 symbols trong cùng một Quý (`YYYY-Q1..Q4`)** thành một bảng PyArrow duy nhất rồi mới gọi `table.append()`.
   - Nhờ bảng Iceberg được phân vùng theo `days(timestamp)`, mỗi ngày UTC chỉ sinh ra đúng **1 file Parquet hợp nhất** chứa đủ 68 symbols.
   - **Kết quả**: Nạp **104,706,818 dòng** vào `bronze.crypto_bars` chỉ sinh ra đúng **2,071 file Parquet** (giảm **41 lần** số file!).

### C. Luồng Biến Đổi Từ `bronze.crypto_bars` Sang `lakehouse.silver.bars` (`bronze_to_silver.py`)
1. **Streaming theo từng Tháng lịch (`YYYY-MM`)**:
   - Đọc dữ liệu từ `bronze.crypto_bars` theo từng cửa sổ 1 tháng (`[YYYY-MM-01T00:00:00+00:00, next_month)`), mỗi tháng khoảng `1.0M – 2.4M` dòng trên toàn bộ 68 symbols (`Peak RAM < 800 MB`).
   - Hỗ trợ **Monthly Checkpoint Resume**: Tự động kiểm tra xem tháng `YYYY-MM` đã có trong `silver.bars` chưa; nếu có rồi thì `SKIP` ngay lập tức (trừ khi truyền cờ `--force-replay`).
2. **Xử lý Vector hóa trong DuckDB In-Process**:
   - **Lọc đủ các điều kiện toán học OHLCV và tính toàn vẹn giao dịch**:
     $$\text{open, high, low, close} > 0 \;\land\; \text{high} \ge \text{low} \;\land\; \text{high} \ge \text{open} \;\land\; \text{high} \ge \text{close} \;\land\; \text{low} \le \text{open} \;\land\; \text{low} \le \text{close} \;\land\; \text{volume} \ge 0 \;\land\; (\text{trade\_count IS NULL} \lor \text{trade\_count} \ge 0)$$
   - **Gán trọng số ưu tiên động (`priority`)**:
     - `source = 'correction'` $\to$ `priority = 3`
     - `source = 'historical'` $\to$ `priority = 2`
     - `source = 'realtime'` $\to$ `priority = 1`
   - **Khử trùng lặp tuyệt đối theo khóa chính**:
     ```sql
     QUALIFY ROW_NUMBER() OVER (
         PARTITION BY exchange, raw_symbol, timestamp
         ORDER BY priority DESC, ingested_at DESC
     ) = 1
     ```
3. **Kết quả thực tế**:
   - Từ **104,706,818 dòng** ở `bronze.crypto_bars`, tầng Silver đã lọc bỏ **48,982 dòng** trùng lặp/vi phạm 7 quy tắc OHLCV và ghi chính xác **104,657,836 dòng** sạch vào **2,058 file Parquet** trong `lakehouse.silver.bars` với tốc độ **986,372 dòng/giây**!

---

## 5. Kiến Trúc Phase 2 Hybrid: 2 Medallion Song Song Hội Tụ Tại Cloud Silver (Option 1)

Để kết hợp luồng Historical/REST tại Local với luồng WebSocket 24/7 trên Cloud mà **không cần đồng bộ qua lại 2 chiều** và **không bao giờ bị xung đột khóa Iceberg Catalog qua mạng WAN**, hệ thống áp dụng mô hình **Option 1 (Local Lightweight Producer — Single Cloud Gold)**:

```text
LOCAL (`s3://lakehouse/`)                          CLOUD (`s3://<cloud>-lakehouse/`)
─────────────────────────                          ─────────────────────────────────
raw/ ─► staging/canonical/                         WS Alpaca + WS Binance ─► Kafka
     ─► bronze.crypto_bars                                                    │
     ─► silver.bars (priority = 2)                                            ▼
          │ (Dừng tại silver.bars,                                    bronze.rt_bars
          │  KHÔNG tạo Gold ở Local)                               (priority = 1, 3)
          ▼                                                                   │
  publish_confirmed_parquet.py                                                │
          │                                                                   ▼
          └──► confirmed/ (Parquet + _manifest.json) ───────────► Cloud Single-Writer Convergence Service (AWS)
               (+ Kế hoạch Cold Bootstrap 104.66M dòng 1 lần đầu)               (Single Writer)
                                                                              │
                                                                              ▼
                                                                        silver.bars
                                                                   (Best Truth: 1 + 2 + 3)
                                                                              │
                                                                              ▼
                                                                   gold.* (Single Source of Truth)
```

1. **Tại sao Local dừng ở `silver.bars` (Option 1 — Chống Split-Brain)?**
   - Trong kiến trúc đề xuất cho Phase 2, luồng WebSocket (`priority = 1` và đặc biệt là bản đính chính `priority = 3`) chỉ đi vào Cloud (`bronze.rt_bars`). Nếu Local tự tính bảng `gold.*` hàng giờ mà thiếu các bản ghi `priority = 3` trên Cloud, số liệu Gold giữa Local và Cloud sẽ lệch nhau (Split-Brain).
   - Do đó, Local đóng vai trò **Lightweight Producer**: dừng tại `silver.bars` và xuất bản lát cắt Parquet bất biến + `_manifest.json` sang `confirmed/`.
2. **Tại sao kiến trúc Cloud đề xuất thu thập WebSocket từ cả 2 sàn (`WS Alpaca + WS Binance`)?**
   - Bảng chênh lệch giá `v_daily_cross_exchange_normalized_spread` và API `query_minute_spread.py` thực hiện `INNER JOIN` giữa Alpaca và Binance. Nếu chỉ thu thập WebSocket của 1 sàn, 60 phút gần nhất sẽ bị rỗng (`NULL`) khi chờ REST của sàn kia.
   - Hơn nữa, Binance chiếm hơn **98% thanh khoản khám phá giá** (khối lượng BTC gấp **50.4 lần** Alpaca), và Binance WebSocket hoàn toàn miễn phí.
