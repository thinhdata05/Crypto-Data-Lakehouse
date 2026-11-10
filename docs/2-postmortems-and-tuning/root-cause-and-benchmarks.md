# Phân Tích Nguyên Nhân Gốc Rễ (Post-Mortems), Tối Ưu Hóa Hiệu Năng & Số Liệu Benchmark Thực Tế

> **Tài liệu kỹ thuật chuyên sâu**: Ghi nhận toàn bộ các sự cố kỹ thuật (RCA), công thức tính toán bộ nhớ/tài chính bằng toán học, giải pháp kiến trúc đã áp dụng và bảng số liệu đo lường thực nghiệm (Before vs. After) trên nền tảng **Multi-Exchange Crypto Lakehouse Platform (`s3://lakehouse/`)**.

---

## 1. Post-Mortem #1: Tràn Bộ Nhớ JVM Heap Khi Ghi Đa Phân Vùng (`Spark PartitionedFanoutWriter OOM`)

### A. Triệu Chứng (Symptom)
Khi sử dụng Apache Spark (`INSERT INTO` / `DataFrameWriter`) để ghi hơn 100 triệu dòng dữ liệu lịch sử (2021–2026) vào bảng Iceberg phân vùng theo ngày (`PARTITIONED BY (days(timestamp))`), container `lakehouse-spark-iceberg` (giới hạn 2.5 GB – 4 GB RAM) liên tục bị sập với lỗi:
```text
java.lang.OutOfMemoryError: Java heap space
at org.apache.iceberg.spark.source.SparkWrite$PartitionedFanoutWriter.write(...)
```

### B. Phân Tích Nguyên Nhân Gốc Rễ (Root Cause Analysis — Công Thức Toán Học)
1. Dữ liệu lịch sử trải dài từ `2021-01-01` đến `2026-08` tương ứng với:
   $$N_{\text{partitions}} \approx 2,058 \text{ phân vùng ngày (UTC days)}$$
2. Nếu sử dụng `PartitionedFanoutWriter` mà không sắp xếp toàn cục (Global Range Sort), mỗi Spark task mở đồng thời các bộ đệm Parquet (`ParquetRowGroupWriter`, mặc định $B_{\text{row\_group}} = 64\text{ MB}$) cho từng phân vùng ngày xuất hiện trong luồng dữ liệu:
   $$\text{Memory Pressure}_{\text{Fanout}} = N_{\text{partitions}} \times B_{\text{row\_group}} = 2,058 \times 64\text{ MB} \approx 131.7\text{ GB RAM}$$
3. Ngay cả khi bật `write.distribution-mode = 'range'`, bước `Exchange rangepartitioning(days(timestamp))` trên 104.7 triệu dòng trong JVM vẫn gây áp lực bộ nhớ đệm (shuffle spill + object serialization overhead) vượt quá ngưỡng Heap cho phép của Docker container trên máy cá nhân.

### C. Giải Pháp Kiến Trúc (Zero-JVM-OOM PyIceberg + DuckDB Streaming)
Loại bỏ hoàn toàn JVM khỏi đường dẫn ETL nặng; chuyển sang kiến trúc **PyIceberg (Native C++ Arrow) + DuckDB In-Process Vectorized Engine**:
- Sử dụng PyArrow đọc trực tiếp file Parquet theo từng cửa sổ thời gian căn chỉnh với phân vùng (Quý `YYYY-Q1..Q4` cho Bronze, Tháng `YYYY-MM` cho Silver).
- Giới hạn bộ nhớ DuckDB (`SET memory_limit='1GB'`, `SET threads=4`) và giải phóng vùng nhớ C++ ngay sau mỗi cửa sổ bằng `pa.default_memory_pool().release_unused()`.

---

## 2. Post-Mortem #2: Bùng Nổ 84,149 File Nhỏ & Phình Ổ Đĩa Ảo WSL2 (`docker_data.vhdx` 86.35 GB)

### A. Triệu Chứng (Symptom)
1. Ở thiết kế nạp theo từng mã tài sản (vòng lặp ngoài duyệt qua 68 symbols, mỗi symbol gọi `table.append()` cho toàn bộ lịch sử 5 năm), bảng Iceberg sinh ra tới **84,149 file Parquet nhỏ/bảng** (tổng cộng hơn **330,000 file** cả metadata `xl.meta` của MinIO).
2. File ổ đĩa ảo của Docker Desktop trên Windows (`E:\DockerWSL\docker_data.vhdx`) phình lên tới **86.35 GB**, làm ổ `E:` (100 GB) chỉ còn trống `0.30 GB` và khiến MinIO dừng ghi (`XMinioStorageFull`).
3. Khi chạy `diskpart -> compact vdisk` lần đầu tiên sau khi xóa dữ liệu cũ, dung lượng file `.vhdx` không giảm một MB nào.

### B. Phân Tích Nguyên Nhân Gốc Rễ (RCA)
1. **Lệch trục giữa Vòng lặp nạp (Symbol-First) và Khóa phân vùng (Day-First)**:
   - Bảng Iceberg được phân vùng theo `PARTITIONED BY (days(timestamp))`.
   - Khi nạp từng symbol một ($S = 68$) qua $D \approx 1,300$ ngày giao dịch, mỗi lần `table.append(symbol_df)` buộc PyIceberg phải tạo 1 file Parquet riêng cho mỗi ngày của symbol đó:
     $$N_{\text{files}} = \sum_{s=1}^{68} D_s \approx 84,149 \text{ Parquet files/bảng}$$
   - Trong hệ thống lưu trữ đối tượng MinIO trên `ext4`, mỗi object tạo một thư mục con kèm file `xl.meta` và `part.1` (tối thiểu 2 block 4 KB + inode overhead), nhân gấp 3–4 lần dung lượng đĩa vật lý.
2. **Tại sao xóa file trong Linux mà `compact vdisk` không giảm dung lượng?**:
   - Lệnh `rm -rf` trong Linux (`ext4` trên `/dev/sde`) chỉ hủy liên kết inode trong bảng mục lục chứ không ghi số `0` đè lên 61 GB các block dữ liệu cũ.
   - Nếu không chạy lệnh `fstrim` bên trong Linux VM để phát tín hiệu SCSI `UNMAP / TRIM`, Hyper-V trên Windows vẫn thấy các sector đó chứa bit khác `0` nên không thể thu hồi khi chạy `compact vdisk`.

### C. Giải Pháp & Kết Quả Thực Nghiệm (Before vs. After)
1. **Đảo trục vòng lặp sang Gom nhóm theo Cửa sổ Thời gian (Partition-Aligned Window Batching)**:
   - [`raw_to_bronze.py`](../../src/pipeline/bronze/raw_to_bronze.py): Gom toàn bộ 68 symbols theo từng **Quý (`YYYY-Q1..Q4`)** trước khi gọi `table.append()`.
   - [`bronze_to_silver.py`](../../src/pipeline/silver/bronze_to_silver.py): Quét và khử trùng lặp toàn bộ 68 symbols theo từng **Tháng (`YYYY-MM`)**.
   - Nhờ đó, mỗi phân vùng ngày UTC chỉ sinh ra đúng **1 file Parquet hợp nhất** chứa đủ 68 symbols!
2. **Thu hồi dung lượng VHDX bằng `fstrim -a -v` + `compact vdisk`**:
   - Chạy `docker builder prune -af` + `fstrim -a -v` trên `/dev/sde` trong Linux VM (trim hơn 61 GB block trống), sau đó chạy `wsl --shutdown` và `diskpart -> compact vdisk`.

| Chỉ Số Đo Lường | Trước Tối Ưu (Symbol-by-Symbol) | Sau Tối Ưu (Partition-Aligned Windows) | Mức Cải Thiện |
| :--- | :--- | :--- | :--- |
| **Số file Parquet ở `bronze.crypto_bars`** | `84,149` files (`100.65M` dòng) | **`2,071` files** (`104,706,818` dòng) | **Giảm 40.6 lần** |
| **Số file Parquet ở `silver.bars`** | `84,149` files $\times$ 2 bảng Silver | **`2,058` files** (`104,657,836` dòng, 1 bảng duy nhất) | **Giảm 81.7 lần** |
| **Tốc độ xử lý `bronze_to_silver.py`** | ~71,000 rows/s (Peak RAM `2.8 GB`) | **`986,372` rows/s** (`106.1s`, Peak RAM `< 800 MB`) | **Nhanh gấp 13.9 lần** |
| **Thời gian chạy `data_validation.py`** | ~95s (Peak RAM `2.4 GB`) | **`22.7s`** (Quarterly Projection, Peak RAM `< 500 MB`) | **Nhanh gấp 4.2 lần** |
| **Dung lượng `E:\DockerWSL\docker_data.vhdx`** | **`86.35 GB`** (Ổ `E:` còn `0.30 GB`) | **`39.32 GB`** (Ổ `E:` còn trống **`47.34 GB`**) | **Tiết kiệm 47.03 GB** |

---

## 3. Post-Mortem #3: Khôi Phục 3.65 Triệu Dòng Alpaca Bị Lỗi Lệch Kiểu `int64` vs. `double`

### A. Triệu Chứng (Symptom)
Khi chạy `src/preprocessing/alpaca.py` trên 38,677 file Parquet ngày của Alpaca, có **123 tháng** bị báo lỗi và bỏ qua với thông báo:
```text
pyarrow.lib.ArrowInvalid: Unable to merge: Field volume has incompatible types: double vs int64
```

### B. Nguyên Nhân Gốc Rễ (RCA)
Trong dữ liệu thô theo ngày của Alpaca, những ngày tài sản mới niêm yết hoặc có khối lượng bằng `0` nguyên tuyệt đối thì cột `volume` được lưu dưới kiểu `int64`, trong khi các ngày bình thường lưu kiểu `double`. Hàm `pa.concat_tables(tables)` mặc định yêu cầu kiểu dữ liệu khớp tuyệt đối giữa các bảng ngày trong cùng một tháng.

### C. Giải Pháp (Resolution)
Cập nhật `pa.concat_tables(tables, promote_options="permissive")` trong cả [`src/preprocessing/alpaca.py`](../../src/preprocessing/alpaca.py) và [`src/preprocessing/binance.py`](../../src/preprocessing/binance.py) để PyArrow tự động nâng cấp kiểu `int64 -> double` không mất dữ liệu.
- **Kết quả**: Khôi phục thành công **123 tháng (`3,653,460` dòng)** của Alpaca, nâng tổng số dòng chuẩn hóa từ `100,650,539` lên **`104,706,818` dòng** với **`0` lỗi**.

---

## 4. Post-Mortem #4: Lỗi Hậu Tố `Z` Trong PyIceberg Trên Python 3.10 & Chuẩn Hóa `raw_symbol` Alpaca Ở Incremental DAG

### A. Triệu Chứng (Symptom)
Khi kiểm thử thực tế DAG [`lakehouse_incremental_hourly`](../../airflow/dags/lakehouse_incremental_hourly_dag.py) trên Airflow:
1. Task `incremental_reconcile_merge_sync` báo lỗi: `ValueError: Invalid timestamp with zone: 2026-11-08T04:00:00Z (must be ISO-8601)`.
2. Khi truyền `--alpaca-symbols "BTC/USD,ETH/USD"` vào `rest_fetcher.py`, số lượng symbol trong `silver.bars` tăng từ `68` lên `70`.

### B. Nguyên Nhân Gốc Rễ (RCA)
1. Container `lakehouse-spark-iceberg` sử dụng Python 3.10. Hàm `datetime.fromisoformat()` của Python 3.10 (được PyIceberg gọi nội bộ khi biên dịch biểu thức lọc `TimestamptzType`) chỉ chấp nhận định dạng múi giờ `+00:00` chứ chưa hỗ trợ hậu tố chữ `Z` (chỉ có từ Python 3.11+).
2. Trong `alpaca.py`, `raw_symbol` của Alpaca được lấy từ tên thư mục `symbol=BTC_USD` (dùng dấu gạch dưới `_`), trong khi API REST của Alpaca nhận tham số `BTC/USD`. Nếu `rest_fetcher.py` lưu nguyên `"BTC/USD"` vào cột `raw_symbol`, khóa chính `(exchange, raw_symbol, timestamp)` sẽ coi `BTC/USD` và `BTC_USD` là 2 mã khác nhau.

### C. Giải Pháp (Resolution)
1. Bổ sung hàm chuẩn hóa `_normalize_pyiceberg_ts()` trong [`incremental_sync.py`](../../src/pipeline/incremental/incremental_sync.py) và [`publish_confirmed_parquet.py`](../../src/pipeline/incremental/publish_confirmed_parquet.py) để tự động chuyển hậu tố `Z` thành `+00:00` (cho bảng `TimestamptzType`) hoặc chuỗi naive (cho bảng `TimestampType`).
2. Chuẩn hóa `canonical_raw_sym = clean_raw_symbol(sym).replace("/", "_")` trong [`rest_fetcher.py`](../../src/pipeline/incremental/rest_fetcher.py) để mọi bản ghi REST của Alpaca luôn mang `raw_symbol` dạng `BTC_USD`, `ETH_USD` đồng nhất 100% với 68 symbols gốc.

---

## 5. Phân Tích Rủi Ro Tỷ Giá Stablecoin (De-Peg Risk) & Chuẩn Hóa Tại Tầng Serving

### A. Vấn Đề Nghiệp Vụ Tài Chính
- Alpaca yết giá bằng `USD` (tiền pháp định), trong khi Binance yết giá chủ đạo bằng `USDT` hoặc `USDC` (Stablecoin).
- Nếu giả định cứng $1\text{ USDT} = 1\text{ USD}$, trong các sự kiện biến động thị trường (ví dụ `USDC` de-peg về `0.88 USD` vào tháng 03/2023 hoặc `USDT` lệch về `0.985 USD`), chênh lệch giá tính theo công thức thô $\text{Close}_{\text{Alpaca}} - \text{Close}_{\text{Binance}}$ sẽ báo hiệu cơ hội Arbitrage giả mạo.

### B. Giải Pháp Toán Học Tại Tầng Phục Vụ (`phase2-cloud-setup-spec.md` & `query_minute_spread.py`)
1. Giữ nguyên đơn vị tiền tệ gốc trong `bronze.crypto_bars` và `silver.bars`.
2. Trích xuất chuỗi tỷ giá thực tế `USDT-USD` và `USDC-USD` qua View `v_fiat_stablecoin_rates`.
3. Chuẩn hóa giá Binance về `USD` tại thời điểm truy vấn:
   $$P_{\text{Binance, USD}}(t) = P_{\text{Binance, USDT}}(t) \times R_{\text{USDT/USD}}(t)$$
   $$\text{Normalized Spread}_{\text{USD}}(t) = P_{\text{Alpaca, USD}}(t) - P_{\text{Binance, USD}}(t)$$
