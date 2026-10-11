---
description: Docker container execution protocols, JVM thread protection, MinIO storage guardrails, and pipeline idempotency
globs: ["docker/**", "docker-compose.yml", "airflow/**", "config/**", "scripts/**", "sql/**"]
---

# 03. Safety, DevOps & Execution Guardrails

Quy chuẩn an toàn khi thực thi lệnh hệ thống, bảo vệ tài nguyên Docker/JVM, quản lý lưu trữ MinIO và bảo đảm tính lũy kế (idempotency).

---

## 1. Containerized Execution Environment (Môi trường thực thi Docker)
- Tất cả các thao tác tương tác với Apache Spark, Iceberg REST Catalog, Airflow và MinIO phải được thực hiện thông qua Docker containers trên mạng nội bộ `lakehouse-net`:
  - **Spark SQL**:
    ```bash
    docker exec -i lakehouse-spark-iceberg spark-sql -f /opt/spark/work-dir/<sql_file>
    ```
  - **Spark Submit**:
    ```bash
    docker exec -it lakehouse-spark-iceberg spark-submit --master local[*] <script.py>
    ```
  - **Airflow CLI**:
    ```bash
    docker exec -it lakehouse-airflow-webserver airflow dags trigger <dag_id>
    docker exec -it lakehouse-airflow-webserver airflow tasks state <dag_id> <task_id> <execution_date>
    ```
- **Quy tắc an toàn**: Tuyệt đối không thay đổi cấu hình mạng Docker `lakehouse-net` hoặc kill container đột ngột khi đang diễn ra tác vụ ghi dữ liệu vào Iceberg.

---

## 2. JVM Thread Pool & Memory Protection (Bảo vệ Spark trên 100M+ dòng)
- Bảng `gold.curated_crypto_bars` chứa hơn 100,650,539 bản ghi trải dài trên 1,584 phân vùng ngày.
- **QUY TẮC BẮT BUỘC KHI TRUY VẤN**:
  - Mọi câu lệnh ad-hoc kiểm tra hoặc `JOIN` phân tích liên sàn (Cross-Exchange Joins) **BẮT BUỘC PHẢI CÓ ĐIỀU KIỆN LỌC PHÂN VÙNG THEO TIMESTAMP**:
    ```sql
    WHERE timestamp >= '2024-09-01 00:00:00' AND timestamp < '2024-09-08 00:00:00'
    ```
  - **CẢNH BÁO NGUY HIỂM CHẾT NGƯỜI**: Chạy truy vấn không có mệnh đề `WHERE timestamp` trên toàn bộ 100M dòng sẽ kích hoạt hàng nghìn tasks đồng thời trong môi trường Docker tài nguyên hữu hạn, dẫn đến lỗi:
    - `java.util.concurrent.RejectedExecutionException` (Executor thread pool kiệt quệ).
    - JVM OutOfMemoryError / Treo toàn bộ Spark container.

---

## 3. Storage Safety & Protected Artifacts (Bảo vệ lưu trữ)
- **MinIO Object Storage (`s3://warehouse/`)**:
  - Tuyệt đối không thực hiện xóa xô (bucket deletion) hoặc xóa thư mục hàng loạt trong `s3://warehouse/` nếu không có sự đồng ý rõ ràng của người dùng.
- **Tài sản Benchmark Được Bảo Vệ**:
  - Hai bảng dữ liệu kiểm thử nằm tại `warehouse/bronze/`:
    - `test_spark_clustered`
    - `test_spark_range`
  - Đây là 2 bảng dữ liệu đối chứng hiệu năng thực nghiệm. **TUYỆT ĐỐI KHÔNG ĐƯỢC XÓA** trong quá trình bảo trì hay dọn dẹp thư mục.

---

## 4. Pipeline Idempotency & Replayability Standard (Tính lũy kế an toàn)
- Mọi pipeline, Airflow DAG, và script ETL phải được thiết kế để có thể chạy lại nhiều lần (Safe Replay) mà không sinh ra sai lệch:
  1. **Zero-Duplicate Invariant**: Chạy lại cùng một batch incremental phải cho kết quả số dòng trùng lặp bằng 0 (`duplication_count = 0`).
  2. **ACID MERGE INTO**: Luôn so sánh `priority` và đối chiếu Business Key `(exchange, raw_symbol, timestamp)` khi cập nhật.
  3. **DDL Scripting**: Luôn dùng cú pháp `CREATE OR REPLACE VIEW` hoặc `CREATE TABLE IF NOT EXISTS`.
  4. **Iceberg Snapshot Retention**: Khi chạy thủ tục dọn dẹp `expire_snapshots`, phải luôn giữ lại tối thiểu 2 snapshot (`retain_last = 2`) để bảo đảm an toàn cho các tác vụ đọc đồng thời.

---

## 5. Security & Secret Isolation
- Không bao giờ commit file `.env` hoặc hardcode thông tin nhạy cảm (MinIO Root Password, Postgres Credentials, Alpaca API Keys, Binance Secret Keys) vào repository code hoặc tài liệu markdown.
- Toàn bộ tham số nhạy cảm phải được truyền qua biến môi trường hoặc Docker Compose environment configs.
