# Runbook: Kiểm Tra & Tiến Hóa Phân Vùng Iceberg Catalog (`verify_and_migrate_catalog.py`)

## 1. Mục Đích & Bối Cảnh
Theo quy chuẩn bất biến của Lakehouse ([`.agent/rules/01-lakehouse-invariants.md`](../../.agent/rules/01-lakehouse-invariants.md)), tất cả các bảng dữ liệu nến 1 phút trên hệ thống:
- **BẮT BUỘC** phân vùng theo ngày: `PARTITIONED BY (days(timestamp))` (hoặc `days(trade_date)`, `days(processed_at)`, `days(created_at)` cho các bảng tổng hợp và vận hành).
- **TUYỆT ĐỐI CẤM** phân vùng theo `exchange`, `raw_symbol` hoặc `canonical_symbol` (nhằm ngăn chặn thảm họa bùng nổ hàng trăm nghìn file nhỏ).

Trong Apache Iceberg, lệnh `CREATE TABLE IF NOT EXISTS` **không tự động sửa lại** cấu trúc phân vùng (Partition Spec) của các bảng đã tồn tại từ trước. Do đó, dự án trang bị công cụ tự động [`src/utils/verify_and_migrate_catalog.py`](../../src/utils/verify_and_migrate_catalog.py) để kiểm toán và tiến hóa phân vùng (In-place Partition Evolution) trực tiếp trên REST Catalog.

---

## 2. Ma Trận Phân Vùng Chuẩn Trên Toàn Hệ Thống

| Môi Trường | Namespace / Bảng | Partition Spec Chuẩn | Trường Cấm Phân Vùng |
| :--- | :--- | :--- | :--- |
| **Local & Cloud Control Plane** | `lakehouse.ops.ingestion_manifest` | `days(processed_at)` | `exchange`, `raw_symbol` |
| **Local & Cloud Control Plane** | `lakehouse.ops.pipeline_audit` | `days(created_at)` | `dag_id`, `status` |
| **Local Data Plane** | `lakehouse.bronze.crypto_bars` | `days(timestamp)` | `exchange`, `raw_symbol` |
| **Local & Cloud Data Plane** | `lakehouse.silver.bars` | `days(timestamp)` | `exchange`, `raw_symbol` |
| **Cloud Data Plane** | `lakehouse.bronze.rt_bars` | `days(timestamp)` | `exchange`, `raw_symbol` |
| **Cloud Data Plane (Gold)** | `lakehouse.gold.agg_daily_crypto_ohlcv` | `days(trade_date)` | `exchange`, `canonical_symbol` |
| **Cloud Data Plane (Gold)** | `lakehouse.gold.agg_daily_cross_exchange_spread` | `days(trade_date)` | `exchange`, `base_currency` |
| **Cloud Data Plane (Gold)** | `lakehouse.gold.dim_symbol` | Unpartitioned (SCD Type 1) | `exchange`, `raw_symbol` |
| **Cloud Data Plane (Gold)** | `lakehouse.gold.dim_date` | Unpartitioned | `year`, `month` |

---

## 3. Lệnh Thực Thi Kiểm Toán & Tiến Hóa Tự Động

### 3.1. Chạy Kiểm Tra & Tự Động Sửa Partition Spec
```bash
docker exec -w /home/iceberg lakehouse-spark-iceberg python3 src/utils/verify_and_migrate_catalog.py
```

### 3.2. Chế Độ Chỉ Kiểm Tra (Dry Run / Không Sửa Đổi)
```bash
docker exec -w /home/iceberg lakehouse-spark-iceberg python3 src/utils/verify_and_migrate_catalog.py --no-evolve
```

---

## 4. Cơ Chế Hoạt Động
1. Kết nối tới Iceberg REST Catalog (`http://iceberg-rest:8181`) trên bucket `s3://lakehouse/`.
2. Quét metadata của toàn bộ các bảng trong `ops`, `bronze`, `silver` (và `gold` khi chạy trên Cloud).
3. Nếu phát hiện bất kỳ trường phân vùng bị cấm nào (ví dụ `exchange` hoặc `raw_symbol`), công cụ tự động gọi API Partition Evolution của PyIceberg:
   ```python
   with table.update_spec() as update:
       update.remove_field(field.name)
   ```
4. Trả về mã thoát `0` khi 100% bảng tuân thủ chuẩn phân vùng, hoặc `1` nếu có lỗi cần can thiệp.
5. Công cụ này được tích hợp tự động vào Task `init_iceberg_tables` trong DAG [`airflow/dags/lakehouse_historical_backfill_dag.py`](../../airflow/dags/lakehouse_historical_backfill_dag.py).
