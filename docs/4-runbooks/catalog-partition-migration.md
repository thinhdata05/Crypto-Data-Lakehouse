# Runbook: Iceberg Catalog Partition Spec Verification & Migration

## 1. Mục đích & Bối cảnh
Theo **Lakehouse Invariant 01** (`.agent/rules/01-lakehouse-invariants.md`), các bảng fact 1-minute trong Lakehouse:
- **BẮT BUỘC** phân vùng theo thời gian: `PARTITIONED BY (days(timestamp))` (hoặc `days(trade_date)` / `days(created_at)` / `days(processed_at)`).
- **TUYỆT ĐỐI CẤM** phân vùng theo `exchange` hoặc `raw_symbol`.

Tuy nhiên, trong Apache Iceberg, lệnh `CREATE TABLE IF NOT EXISTS` **không làm thay đổi** partition spec của các bảng đã tồn tại trong catalog trước đó. Nếu môi trường đã tạo bảng cũ chứa `PARTITIONED BY (days(timestamp), exchange)`, việc sửa file DDL `.sql` là chưa đủ để cập nhật catalog.

Runbook này cung cấp quy trình và công cụ tự động để kiểm tra, phát hiện và evolve partition spec trực tiếp trên Iceberg Catalog mà không cần xóa dữ liệu.

---

## 2. Invariant Partition Spec Ma trận chuẩn

| Tầng / Bảng | Partition Transform Chuẩn | Cấm Phân Vùng Theo |
| :--- | :--- | :--- |
| `lakehouse.bronze.crypto_bars` | `days(timestamp)` | `exchange`, `raw_symbol` |
| `lakehouse.bronze.source_manifest` | `days(processed_at)` | `exchange`, `raw_symbol` |
| `lakehouse.silver.crypto_bars` | `days(timestamp)` | `exchange`, `raw_symbol` |
| `lakehouse.silver.curated_crypto_bars` | `days(timestamp)` | `exchange`, `raw_symbol` |
| `lakehouse.gold.curated_crypto_bars` | `days(timestamp)` | `exchange`, `raw_symbol` |
| `lakehouse.gold.agg_daily_crypto_ohlcv` | `days(trade_date)` hoặc unpartitioned | `exchange`, `canonical_symbol` |
| `lakehouse.gold.agg_daily_cross_exchange_spread` | `days(trade_date)` hoặc unpartitioned | `exchange`, `base_currency` |
| `lakehouse.gold.dim_symbol` | Unpartitioned (SCD Type 1 Dimension) | `exchange`, `raw_symbol` |
| `lakehouse.gold.pipeline_audit` | `days(created_at)` | `dag_id`, `status` |

---

## 3. Công cụ kiểm tra & Migrate tự động

Script: [`src/utils/verify_and_migrate_catalog.py`](file:///e:/project_hybrid_lakehouse_crypto/src/utils/verify_and_migrate_catalog.py)

### 3.1. Chạy trên Docker Container (Khuyến nghị)
```bash
docker exec -w /home/iceberg lakehouse-spark-iceberg python src/utils/verify_and_migrate_catalog.py
```

### 3.2. Chế độ kiểm tra chỉ đọc (Dry Run / No Evolve)
```bash
docker exec -w /home/iceberg lakehouse-spark-iceberg python src/utils/verify_and_migrate_catalog.py --no-evolve
```

---

## 4. Cơ chế hoạt động của Script
1. Kết nối vào REST Catalog (`http://rest:8181`).
2. Tải metadata của từng bảng và kiểm tra danh sách partition fields trong spec hiện hành.
3. Nếu phát hiện trường cấm (ví dụ `exchange`), script tự động gọi PyIceberg Partition Evolution API:
   ```python
   update = table.update_spec()
   update.remove_field("exchange")
   update.commit()
   ```
4. Xác nhận lại spec mới và trả về mã exit code:
   - `0`: Tất cả các bảng 100% tuân thủ invariant.
   - `1`: Có bảng lỗi hoặc không thể evolve.

---

## 5. Tích hợp trong Pipeline & Airflow
Task `init_iceberg_tables` trong DAG `lakehouse_historical_backfill` đã được xâu chuỗi:
```bash
spark-sql -f /home/iceberg/sql/ddl/01_create_iceberg_tables.sql
python3 /home/iceberg/src/utils/verify_and_migrate_catalog.py
```
Điều này đảm bảo rằng mỗi khi pipeline khởi chạy, catalog luôn được kiểm tra và chuẩn hóa tự động trước khi bước nạp dữ liệu bắt đầu.
