# Storage Architecture & Warehouse Clarification Note

> **Tài liệu làm rõ kiến trúc lưu trữ**: Phân biệt giữa thư mục cục bộ `warehouse/` (Local Staging) và kho dữ liệu thực sự `s3://warehouse/` (MinIO Object Storage) trong hệ thống Hybrid Lakehouse Crypto.

---

## 1. Ngữ cảnh & Vấn đề thường gây hiểu lầm

Khi mới tiếp cận dự án, việc nhìn thấy một thư mục mang tên `warehouse/` nằm ngay tại thư mục gốc máy cục bộ (`E:\project_hybrid_lakehouse_crypto\warehouse`) rất dễ gây ra sự hiểu lầm:
* *Hiểu lầm:* Toàn bộ dữ liệu Data Lakehouse / Data Warehouse đang được lưu trực tiếp trên hệ thống file cục bộ của máy tính.
* *Thực tế:* Đây **không phải** là Data Warehouse, mà chỉ là **Tầng đệm trung chuyển (Staging Zone)**. Kho dữ liệu Lakehouse thực sự được lưu trên MinIO (S3 Object Storage).

---

## 2. Phân tích chi tiết 2 vùng lưu trữ

```
┌────────────────────────────────────────────────────────────────────────┐
│                              HOST MACHINE                              │
│                                                                        │
│   E:\project_hybrid_lakehouse_crypto\warehouse\                        │
│   └── staging\canonical\                                               │
│       ├── alpaca\                                                      │
│       └── binance\                                                     │
│            │ (Transient / Preprocessed Parquet)                        │
│            ▼                                                           │
│   [Mounted to Container as /home/iceberg/warehouse_local]              │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │ Read by PyIceberg / Spark
                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        DOCKER / CLOUD INFRASTRUCTURE                   │
│                                                                        │
│   MinIO Object Storage (s3://warehouse/)                               │
│   ├── bronze.db/                                                       │
│   │   └── crypto_bars/                                                 │
│   │       ├── metadata/ (*.metadata.json, *.avro)                      │
│   │       └── data/ (partitioned parquet by exchange & day)            │
│   ├── silver.db/                                                       │
│   ├── curated_silver.db/                                               │
│   └── gold.db/                                                         │
│                                                                        │
│   ★ ĐÂY MỚI LÀ LAKEHOUSE THỰC SỰ (Managed by Iceberg REST Catalog)    │
└────────────────────────────────────────────────────────────────────────┘
```

### A. Thư mục `warehouse/` tại Local — Bản chất là Staging Area
- **Đường dẫn vật lý:** `E:\project_hybrid_lakehouse_crypto\warehouse\staging\canonical`
- **Mục đích sử dụng:**
  1. Khi chạy bước tiền xử lý (`src/preprocessing/alpaca.py`, `binance.py`) hoặc khi Airflow chạy REST API fetcher (`src/incremental/rest_fetcher.py`), dữ liệu từ `raw/` được phân tích, bóc tách ký hiệu cặp tiền (base/quote), chuyển đổi timestamp sang UTC chuẩn và làm sạch schema.
  2. Dữ liệu chuẩn hóa này được ghi tạm ra các file Parquet tại thư mục này để tạo thành một tầng dữ liệu "canonical staging".
  3. Spark hoặc PyIceberg (`src/spark/bronze/raw_to_bronze.py`) sẽ đọc từ thư mục đệm này để ingest vào bảng Iceberg Bronze.
- **Đặc điểm:**
  - **Tính chất dữ liệu:** Dữ liệu phái sinh, tạm thời (Derived / Transient data).
  - **Độ an toàn:** Hoàn toàn có thể xóa thư mục này đi mà không mất mát dữ liệu gốc, vì bất cứ lúc nào cũng có thể tái tạo lại từ `raw/` bằng script tiền xử lý.

### B. Kho `s3://warehouse/` trên MinIO — Lakehouse Storage thực sự
- **Địa chỉ lưu trữ:** `s3://warehouse/` (được phục vụ bởi container `minio:9000`).
- **Quản lý Catalog:** `lakehouse-iceberg-rest` (Apache Iceberg REST Catalog fixture tại cổng `8181`).
- **Mục đích sử dụng:**
  - Đây là nơi lưu trữ toàn bộ các bảng Medallion chính thức theo chuẩn **Apache Iceberg Table Format**:
    - `bronze.crypto_bars`: Bảng thô hợp nhất đa sàn.
    - `silver.crypto_bars_standardized`: Bảng làm sạch và lọc lỗi.
    - `curated_silver.crypto_bars_curated`: Bảng giải quyết ưu tiên (Priority Merge).
    - `gold.fact_crypto_bars` & `gold.dim_date`: Bảng dữ liệu chiều (Star Schema) phục vụ phân tích.
  - Mỗi bảng chứa đầy đủ:
    - Metadata snapshots (`metadata/*.metadata.json`).
    - Manifest list & manifest files (`metadata/*.avro`).
    - Dữ liệu phân vùng Parquet tối ưu (`data/exchange=.../timestamp_day=.../*.parquet`).

---

## 3. Cơ chế ánh xạ trong `docker-compose.yml`

Trong [docker-compose.yml](../../docker-compose.yml), cấu hình mount thư mục cho Spark container được thiết kế như sau:

```yaml
spark-iceberg:
  volumes:
    - warehouse_data:/home/iceberg/warehouse          # [1] Volume Docker nội bộ của Spark
    - ./warehouse:/home/iceberg/warehouse_local       # [2] Mount thư mục local từ máy Host
```

1. **`warehouse_data:/home/iceberg/warehouse`**: Volume nội bộ của Spark container dùng để lưu cache/scratchpad.
2. **`./warehouse:/home/iceberg/warehouse_local`**: Để tránh xung đột tên với thư mục `/home/iceberg/warehouse` của container, thư mục `./warehouse` từ máy chủ (host) được mount dưới tên định danh rõ ràng là `/home/iceberg/warehouse_local`.

---

## 4. Bảng đối chiếu tóm tắt

| Tiêu chí so sánh | `warehouse/` (Local Host) | `s3://warehouse/` (MinIO) |
| :--- | :--- | :--- |
| **Vị trí vật lý** | Ổ cứng máy chủ (`E:\project_...`) | Docker Named Volume (`minio_data`) |
| **Bản chất kiến trúc** | **Staging Area** (Vùng đệm xử lý) | **Data Lakehouse** (Storage chính thức) |
| **Định dạng dữ liệu** | Parquet thuần túy | Apache Iceberg (Parquet + Avro Metadata) |
| **Các tầng dữ liệu** | Chỉ chứa staging canonical | Đầy đủ: Bronze, Silver, Curated Silver, Gold |
| **Ảnh hưởng khi xóa** | Không mất dữ liệu, tạo lại được | Mất dữ liệu bảng Iceberg & snapshot lịch sử |

---

## 5. Khuyến nghị kiến trúc (Architecture Recommendation)

1. **Về việc đặt tên:** Trong tương lai, nếu thực hiện refactor kiến trúc:
   - Thư mục `./warehouse` trên host nên được đổi tên thành `./staging` hoặc `./data/staging`.
   - Cập nhật ánh xạ mount trong `docker-compose.yml` thành:
     `- ./staging:/home/iceberg/staging`
   - Điều này sẽ loại bỏ hoàn toàn sự nhập nhằng giữa "Staging trên đĩa" và "Warehouse trên Object Storage".
2. **Quản lý dung lượng đĩa:** Sau khi toàn bộ dữ liệu lịch sử đã được nạp thành công vào Iceberg Bronze/Silver (`s3://warehouse/`), bạn có thể dọn dẹp các thư mục con trong `warehouse/staging/canonical/` nếu ổ cứng cần giải phóng dung lượng.

---

## 6. Thiết kế Bucket S3/MinIO: Hiện trạng & Đề xuất Mô hình Multi-Bucket

### A. Hiện trạng của dự án hiện tại (Mô hình Lai - Hybrid)
* **Tầng Raw & Staging:** Lưu tại ổ cứng Local (`E:\project_hybrid_lakehouse_crypto\raw` và `./warehouse/staging`), bind-mount vào container.
* **Tầng Iceberg Lakehouse (Bronze, Silver, Gold):** Lưu tại MinIO Bucket `s3://warehouse/` qua Iceberg REST Catalog.
* **Bucket `lakehouse`:** Đã được tạo sẵn trong `mc` init script nhưng hiện để trống dự phòng.

### B. Đề xuất nâng cấp: Mô hình Multi-Bucket (Chuẩn Production / Enterprise)
Để hệ thống trở thành **100% Cloud-Native** và đạt chuẩn kiến trúc bảo mật IAM doanh nghiệp, lộ trình chuyển đổi đề xuất như sau:

```text
MinIO / AWS S3
├── 📦 lakehouse-raw/                 # [Chỉ Ingestion Service được ghi]
│   ├── alpaca/bars/...
│   └── binance/kline/...
│
├── 📦 lakehouse-warehouse/           # [Trỏ cho Iceberg REST Catalog: s3://lakehouse-warehouse/]
│   ├── bronze/                       # Bảng bronze.crypto_bars
│   ├── silver/                       # Bảng silver.crypto_bars_standardized
│   ├── curated_silver/               # Bảng curated_silver.crypto_bars_curated
│   └── gold/                         # Bảng fact & dimensional models
│
└── 📦 lakehouse-system/              # [Tự động dọn dẹp theo Lifecycle Rules]
    ├── staging/                      # Thay thế thư mục warehouse/ local
    ├── checkpoints/                  # Spark / Engine checkpoints
    └── logs/                         # Airflow remote task logs
```

* **Lợi ích khi nâng cấp:**
  1. Loại bỏ hoàn toàn sự phụ thuộc vào filesystem cục bộ của máy Host.
  2. Phân quyền IAM rõ ràng theo nguyên tắc Least Privilege (Analyst chỉ đọc Gold, ETL ghi Bronze/Silver).
  3. Cài đặt Lifecycle Policy (TTL) an toàn cho logs/staging mà không sợ ảnh hưởng đến dữ liệu nghiệp vụ.

---

## 7. Chiến Lược Di Cư Phase 2 Lên Cloud (AWS): Dual-Catalog Migration (Cách 1)

### A. Mô hình kiến trúc Dual-Catalog
Thay vì upload file thủ công dễ gây lỗi lệch đường dẫn metadata (broken pointer) trong bảng Iceberg, phương pháp chuẩn được lựa chọn là **Dual-Catalog Replication qua PyIceberg**:

```text
[Local Environment]                                  [AWS Cloud Platform]
┌────────────────────────┐                          ┌────────────────────────┐
│  MinIO (s3://warehouse)│                          │  Amazon S3             │
│  └── gold.curated_...  │                          │  └── (crypto-gold-...) │
└───────────┬────────────┘                          └───────────▲────────────┘
            │                                                   │
            │ (1) Scan to Arrow                                 │ (2) Append stream
            ▼                                                   │
   ┌─────────────────┐                                          │
   │  Python Script  │ ─────────────────────────────────────────┘
   │  (PyIceberg)    │
   └────────┬────────┘
            │ (3) Tự động đăng ký Snapshot & Schema
            ▼
┌────────────────────────┐
│  AWS Glue Data Catalog │ ◄────── [Amazon Athena / BI Tools / Analysts]
└────────────────────────┘
```

### B. Mã nguồn triển khai mẫu (Workflow Pattern)
```python
import os
from pyiceberg.catalog import load_catalog

# 1. Kết nối Catalog Local (MinIO)
local_catalog = load_catalog(
    "local_lakehouse",
    **{
        "type": "rest",
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }
)

# 2. Kết nối Catalog Cloud (AWS Glue)
cloud_catalog = load_catalog(
    "aws_glue",
    **{
        "type": "glue",
        "warehouse": "s3://my-aws-crypto-lakehouse-gold/",
        "client.region": "us-east-1",
    }
)

# 3. Đọc dữ liệu từ bảng Gold cục bộ
local_gold = local_catalog.load_table("gold.curated_crypto_bars")
arrow_data = local_gold.scan().to_arrow()

# 4. Ghi trực tiếp sang AWS S3 và tự động tạo metadata chuẩn trên Glue
cloud_gold = cloud_catalog.load_table("gold.curated_crypto_bars")
cloud_gold.append(arrow_data)
```

### C. Lợi thế kỹ thuật & Tối ưu chi phí
1. **Tiết kiệm tối đa chi phí AWS (Zero Compute Cost):** 100% công đoạn tính toán nặng (101 triệu dòng) đã thực hiện xong ở local. AWS chỉ đóng vai trò lưu trữ tĩnh S3 (khoảng $0.023 / GB / tháng) và Glue Catalog (miễn phí 1 triệu metadata requests).
2. **Metadata toàn vẹn 100%:** PyIceberg tự động tạo metadata file với URI `s3://...` chuẩn của AWS, không sợ bị lỗi broken paths như khi copy file thô.
3. **Sẵn sàng truy vấn tức thì (Serverless):** Ngay sau khi hoàn tất, bảng có thể truy vấn ngay lập tức bằng **Amazon Athena** (`SELECT * FROM gold.curated_crypto_bars ...`) mà không cần tốn chi phí dựng cụm EMR hay Spark cluster chạy 24/7.

> **Ghi chú về định hướng thiết kế:** Tài liệu đặc tả ban đầu (`PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md`) mang tính chất tham khảo tổng quan. Trong thực tế triển khai, hệ thống sẽ ưu tiên các giải pháp hiện đại, tối ưu thực chiến (Modern Data Stack: DuckDB, PyIceberg, AWS S3, AWS Glue, Athena) thay vì bị ràng buộc bởi các thiết kế cồng kềnh lý thuyết.


