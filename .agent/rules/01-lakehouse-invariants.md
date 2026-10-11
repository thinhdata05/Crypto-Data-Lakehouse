---
description: Core architectural invariants and storage rules for Apache Iceberg Medallion tables
globs: ["src/spark/**", "sql/**", "airflow/**", "src/preprocessing/**", "src/incremental/**"]
---

# 01. Lakehouse Core Invariants & Storage Rules

Quy tắc nền tảng bắt buộc tuân thủ tuyệt đối cho mọi tác vụ phát triển, tối ưu hóa và sửa đổi pipeline dữ liệu trên Lakehouse.

---

## 1. Primary Business Key & Định danh duy nhất
- **Khóa định danh hợp nhất (Business Key)** duy nhất trên toàn bộ hệ thống là bộ 3 trường:
  $$\text{Business Key} = (\mathbf{exchange}, \mathbf{raw\_symbol}, \mathbf{timestamp})$$
- **QUY TẮC BẮT BUỘC**:
  - Tuyệt đối **KHÔNG ĐƯỢC** sử dụng `canonical_symbol` hoặc `(canonical_symbol, timestamp)` làm khóa định danh lưu trữ cơ sở.
  - Lý do: Một `canonical_symbol` (ví dụ: `BTC`) tồn tại độc lập trên cả Alpaca (`BTC/USD`) và Binance (`BTCUSDT`, `BTCUSDC`). Khóa `raw_symbol` bảo đảm tính duy nhất tuyệt đối và không gây va chạm khóa.

---

## 2. Multi-Exchange Independence (Độc lập đa sàn)
- **Alpaca** và **Binance** là 2 sàn giao dịch có sổ lệnh (order book), thanh khoản và cơ chế khớp lệnh tách biệt hoàn toàn.
- Dữ liệu của cùng một minute bar giữa Alpaca và Binance:
  - **KHÔNG PHẢI là duplicate**.
  - **TUYỆT ĐỐI KHÔNG ĐƯỢC ghi đè** hoặc deduplicate qua lại giữa 2 sàn.
  - Cả hai bản ghi đều phải được lưu trữ đồng thời trong Gold layer để phục vụ phân tích chênh lệch giá (Basis & Arbitrage).

---

## 3. Intra-Exchange Priority Conflict Resolution (Ưu tiên nội bộ sàn)
- Hệ thống áp dụng 3 cấp độ ưu tiên dữ liệu (`priority`):
  - **Mức 3 (Highest)**: Dữ liệu Backfill lịch sử đã qua kiểm định chất lượng toàn diện.
  - **Mức 2 (Medium)**: Dữ liệu định kỳ hàng giờ từ REST API chính thức (`fetch_rest_bars`).
  - **Mức 1 (Lowest)**: Dữ liệu streaming thời gian thực qua WebSocket (chấp nhận rủi ro mất gói, out-of-order).
- **QUY TẮC VÀNG**:
  - Mức độ ưu tiên ($3 > 2 > 1$) **CHỈ ĐƯỢC ÁP DỤNG NỘI BỘ TRONG CÙNG MỘT SÀN (`exchange`)**.
  - Bản ghi `priority = 3` của Binance chỉ được phép ghi đè bản ghi `priority = 2` của Binance.
  - Bản ghi `priority = 3` của Alpaca **KHÔNG ĐƯỢC PHÉP** ghi đè bản ghi của Binance và ngược lại.

---

## 4. Iceberg Table Partitioning Specification
- Tất cả các bảng Medallion (`bronze.crypto_bars`, `silver.crypto_bars`, `silver.curated_crypto_bars`, `gold.curated_crypto_bars`) đều phải sử dụng chiến lược phân vùng:
  $$\text{PARTITIONED BY (days(timestamp))}$$
- **Anti-patterns nghiêm cấm**:
  - **CẤM** phân vùng theo `exchange` hoặc `symbol`: Sẽ gây bùng nổ hàng chục nghìn small files (partition explosion), làm nghẽn Iceberg REST Catalog và suy thoái hiệu năng đọc/ghi.
  - **CẤM** phân vùng theo unconstrained timestamp (giờ, phút).

---

## 5. Deduplication & ACID MERGE INTO Standard
- Khi thực hiện Deduplication tại tầng Staging/Silver (trong DuckDB hoặc Spark):
  ```sql
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY exchange, raw_symbol, timestamp
      ORDER BY priority DESC, created_at DESC
  ) = 1
  ```
- Khi thực hiện `MERGE INTO` trong Apache Iceberg (`src/spark/incremental/incremental_sync.py`):
  ```sql
  MERGE INTO target t
  USING source s
  ON t.exchange = s.exchange
     AND t.raw_symbol = s.raw_symbol
     AND t.timestamp = s.timestamp
  WHEN MATCHED AND s.priority >= t.priority THEN
     UPDATE SET 
        open = s.open,
        high = s.high,
        low = s.low,
        close = s.close,
        volume = s.volume,
        trade_count = s.trade_count,
        vwap = s.vwap,
        priority = s.priority,
        created_at = s.created_at
  WHEN NOT MATCHED THEN
     INSERT *
  ```

---

## 6. Medallion Layer Responsibilities & Immutability
1. **Bronze Layer (`lakehouse.bronze.crypto_bars`)**:
   - Tính chất: **Immutable, Append-Only**.
   - Lưu trữ nguyên dạng dữ liệu thô (raw payload) kèm `ingestion_timestamp` và `source_file`.
   - Tuyệt đối không xóa hay update bản ghi tại Bronze.
2. **Silver Layer (`lakehouse.silver.crypto_bars`)**:
   - Tầng làm sạch (Cleansed), chuẩn hóa kiểu dữ liệu, gắn nhãn `priority` và cờ hợp lệ chất lượng (`is_valid_sanity`).
3. **Curated Silver & Gold Layer (`curated_crypto_bars`)**:
   - Tầng dữ liệu chân lý duy nhất (Authoritative Single Source of Truth).
   - Đảm bảo 100% không trùng lặp khóa định danh (`exchange, raw_symbol, timestamp`), áp dụng ACID MERGE INTO để cập nhật khi có bản ghi ưu tiên cao hơn.

---

## 7. Zero-JVM-OOM Write Architecture
- Đối với các lô nạp dữ liệu lớn (Historical Backfill trên 100M dòng qua 1,584 phân vùng ngày):
  - **Không sử dụng** Spark `PartitionedFanoutWriter` vì gây tràn bộ nhớ JVM Heap (cần hơn 100GB RAM đệm).
  - **Bắt buộc sử dụng** mô hình streaming **PyIceberg** (bộ đệm C++ native) kết hợp **DuckDB** in-process để giữ mức RAM $< 2.8\text{ GB}$ và tốc độ ổn định $71,000 - 129,000\text{ rows/s}$.
