# Runbook: Vận Hành Hệ Thống, Nạp Dữ Liệu & Bảo Trì Lakehouse

> **Nền tảng**: Multi-Exchange Crypto Lakehouse Platform (Apache Iceberg, PyIceberg, DuckDB, Spark SQL, MinIO `s3://lakehouse/`, Airflow)  
> **Đối tượng**: Data Engineers, Platform Engineers, SRE

---

## 1. Quản Lý Dịch Vụ Docker (`docker-compose.yml`)

### A. Kiểm Tra Trạng Thái 6 Containers Cốt Lõi
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```
Danh sách 6 containers chuẩn trên mạng `lakehouse-net`:
- `lakehouse-minio` (Cổng `9000`: S3 API, `9001`: Web Console — quản lý bucket `s3://lakehouse/`)
- `lakehouse-iceberg-rest` (Cổng `8181`: Iceberg REST Catalog)
- `lakehouse-spark-iceberg` (Cổng `8080`: Spark UI, `8888`: Jupyter — môi trường thực thi PyIceberg + DuckDB + Spark SQL CLI)
- `lakehouse-postgres` (Cổng `5432`: Airflow Metadata Database)
- `lakehouse-airflow-webserver` (Cổng `8085`: Airflow Web UI)
- `lakehouse-airflow-scheduler` (Bộ lập lịch Airflow DAGs)

### B. Sao Lưu & Khôi Phục Iceberg REST Catalog DB
Vì container `lakehouse-iceberg-rest` (`apache/iceberg-rest-fixture`) lưu SQLite metadata tại `/tmp/iceberg_catalog.db`, hãy sao lưu ra thư mục `staging/` sau mỗi đợt nạp lớn:
```bash
# Sao lưu Catalog DB ra máy host:
docker cp lakehouse-iceberg-rest:/tmp/iceberg_catalog.db ./staging/iceberg_catalog.db.bak

# Khôi phục nếu container lakehouse-iceberg-rest bị tạo mới (recreate):
docker cp ./staging/iceberg_catalog.db.bak lakehouse-iceberg-rest:/tmp/iceberg_catalog.db
docker restart lakehouse-iceberg-rest
```

---

## 2. Quy Trình Nạp Historical Backfill (104.7 Triệu Dòng) — CLI vs. Airflow

Trong thực tế vận hành cấp độ Senior Data Engineer:
- **Khởi tạo dữ liệu lịch sử 5 năm lần đầu (Initial Bootstrap — `104,706,818` dòng)**: Khuyến nghị chạy trực tiếp qua **CLI theo từng chặng** để tối ưu 100% RAM/CPU cho DuckDB + PyIceberg và theo dõi tiến độ trực tiếp theo từng Quý/Tháng.
- **Chạy kiểm tra lũy kế (Idempotent Replay)**: Có thể kích hoạt qua DAG `lakehouse_historical_backfill` trên Airflow (nhờ cơ chế SHA-256 manifest ở `ops.ingestion_manifest` và Monthly Checkpoint ở `silver.bars`, các file/tháng đã nạp sẽ được `SKIP` an toàn).

### Các Lệnh Chạy Historical Backfill Chuẩn (CLI):
```bash
# Bước 1: Tiền xử lý & gom 87,693 file ngày từ ./raw thành 2,939 file tháng tại ./staging/canonical
python src/preprocessing/alpaca.py --batch-id <batch_uuid>
python src/preprocessing/binance.py --batch-id <batch_uuid>

# Bước 2: Khởi tạo các bảng Iceberg (ops.*, bronze.crypto_bars, silver.bars) trên s3://lakehouse/
docker exec -w /home/iceberg lakehouse-spark-iceberg spark-sql -f /home/iceberg/sql/ddl/01_create_iceberg_tables.sql

# Bước 3: Nạp vào bronze.crypto_bars (Gom theo Quý YYYY-Q1..Q4 -> 2,071 file Parquet theo ngày)
docker exec -w /home/iceberg lakehouse-spark-iceberg python3 src/pipeline/bronze/raw_to_bronze.py --batch-id <batch_uuid>

# Bước 4: Làm sạch 7 quy tắc OHLCV, gán priority=2 & khử trùng lặp vào silver.bars (Gom theo Tháng YYYY-MM -> 2,058 file)
docker exec -w /home/iceberg lakehouse-spark-iceberg python3 src/pipeline/silver/bronze_to_silver.py --batch-id <batch_uuid> --target-table silver.bars

# Bước 5: Kiểm định chất lượng dữ liệu trên silver.bars
docker exec -w /home/iceberg lakehouse-spark-iceberg python3 src/pipeline/quality/data_validation.py --run-id <batch_uuid> --target-table silver.bars
```

---

## 3. Vận Hành Pipeline Incremental Hàng Giờ (`lakehouse_incremental_hourly`)

Theo kiến trúc **Phase 2 Hybrid (Option 1 — Local Lightweight Producer)**, DAG [`lakehouse_incremental_hourly`](../../airflow/dags/lakehouse_incremental_hourly_dag.py) chạy định kỳ hàng giờ (`0 * * * *`) với 6 bước:
1. `build_hourly_window`: Sinh khung giờ UTC chuẩn ISO-8601 (`+00:00`) và mã `batch_id`.
2. `fetch_rest_bars` ([`rest_fetcher.py`](../../src/pipeline/incremental/rest_fetcher.py)): Kéo nến 1 phút mới nhất từ REST API của Binance và Alpaca (`source='historical'`, chuẩn hóa `raw_symbol` Alpaca dạng gạch dưới `BTC_USD`).
3. `incremental_raw_to_bronze` ([`raw_to_bronze.py`](../../src/pipeline/bronze/raw_to_bronze.py)): Kiểm tra SHA-256 trong `ops.ingestion_manifest` và ghi bổ sung vào `lakehouse.bronze.crypto_bars`.
4. `incremental_reconcile_merge_sync` ([`incremental_sync.py`](../../src/pipeline/incremental/incremental_sync.py)): Làm sạch 7 quy tắc OHLCV, gán `priority = 2`, hợp nhất với dữ liệu hiện hữu trong phân vùng ngày tương ứng của `lakehouse.silver.bars` theo quy tắc $3 > 2 > 1$ (`ingested_at DESC`). **Dừng tại `silver.bars`** (không tạo bảng Gold ở Local để chống Split-Brain).
5. `publish_confirmed_parquet` ([`publish_confirmed_parquet.py`](../../src/pipeline/incremental/publish_confirmed_parquet.py)): Xuất 2 file Parquet bất biến (`alpaca_bars.parquet`, `binance_bars.parquet`) kèm file `_manifest.json` (chứa mã SHA-256) vào thư mục `staging/confirmed/batch_id=<uuid>/` để đồng bộ lên Cloud Silver.
6. `validate_incremental` ([`data_validation.py`](../../src/pipeline/quality/data_validation.py)): Kiểm định chất lượng bảng `silver.bars` và ghi nhận `SUCCESS` vào `lakehouse.ops.pipeline_audit`.

### Lệnh Test & Kích Hoạt DAG Incremental Trên Airflow:
```bash
# Chạy kiểm thử toàn trình (End-to-End Test) trên Airflow Scheduler:
docker exec lakehouse-airflow-scheduler airflow dags test lakehouse_incremental_hourly 2026-11-08T07:00:00Z

# Hoặc kích hoạt chạy thực tế trên Airflow Webserver:
docker exec lakehouse-airflow-webserver airflow dags trigger lakehouse_incremental_hourly
```

---

## 4. Bảo Trì Bảng Iceberg & Thu Hồi Dung Lượng Ổ Đĩa Ảo WSL2 (`docker_data.vhdx`)

### A. Dọn Dẹp Snapshot Cũ Trên Iceberg (`retain_last => 2`)
```bash
docker exec lakehouse-spark-iceberg spark-sql -e "
CALL lakehouse.system.expire_snapshots(
    table => 'lakehouse.silver.bars',
    retain_last => 2
);
"
```

### B. Quy Trình Thu Hồi Dung Lượng `E:\DockerWSL\docker_data.vhdx` (Khi Xóa Dữ Liệu Lớn)
Trên Windows WSL2, khi xóa dữ liệu bên trong Linux container, ổ đĩa ảo `docker_data.vhdx` không tự động giảm kích thước trên Windows nếu chưa chạy `fstrim`. Quy trình chuẩn gồm 2 bước:

1. **Bước 1 (Khi Docker Desktop đang bật)**: Phát tín hiệu SCSI `TRIM/UNMAP` cho các block trống trong Linux VM:
   ```powershell
   docker builder prune -af
   docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -n -i fstrim -a -v
   ```
2. **Bước 2 (Trong PowerShell quyền Administrator)**: Tắt WSL và nén file `.vhdx`:
   ```powershell
   wsl --shutdown
   diskpart
   # Trong cửa sổ DISKPART:
   select vdisk file="E:\DockerWSL\docker_data.vhdx"
   attach vdisk readonly
   compact vdisk
   detach vdisk
   exit
   ```

---

## 5. Truy Vấn Chênh Lệch Giá Liên Sàn (Cross-Exchange Spread)

### A. Truy Vấn Cấp Phút Có Khóa Bảo Vệ Phân Vùng (`query_minute_spread.py`)
```bash
docker exec -w /home/iceberg lakehouse-spark-iceberg python3 src/serving/query_minute_spread.py \
    --base-currency BTC \
    --start-time 2026-08-01T00:00:00+00:00 \
    --end-time 2026-08-02T00:00:00+00:00
```
*(API tự động chặn các truy vấn vượt quá 7 ngày và đẩy bộ lọc `timestamp` xuống tận tầng đọc Parquet của `silver.bars` nhằm bảo vệ bộ nhớ).*
