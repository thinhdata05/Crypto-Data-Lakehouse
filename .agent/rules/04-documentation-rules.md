---
description: Automatic documentation update reflex, single source of truth for DDL, and link integrity standards
globs: ["docs/**", "MEMORY.md", "README.md", "sql/ddl/**"]
---

# 04. Documentation & Agent Behavior Rules

Quy chuẩn phản xạ tự động cập nhật tài liệu, nguồn chân lý duy nhất cho mã DDL, và tính toàn vẹn của liên kết trong hệ thống.

---

## 1. Automatic Documentation Reflex (Phản xạ cập nhật tài liệu tự động)
- Bất kỳ khi nào AI Agent thực hiện thay đổi mã nguồn, bổ sung view/table mới, tối ưu hiệu năng hoặc khắc phục sự cố, Agent **BẮT BUỘC PHẢI TỰ ĐỘNG CẬP NHẬT ĐỒNG THỜI**:
  1. [`MEMORY.md`](file:///e:/project_hybrid_lakehouse_crypto/MEMORY.md):
     - Ghi nhận trạng thái dữ liệu mới nhất (số dòng, số symbols, kết quả kiểm toán).
     - Ghi nhận các views, tables mới được bổ sung.
     - Cập nhật tiến độ hoàn thành các hạng mục của Phase hiện tại và mục tiêu Phase tiếp theo.
  2. Thư mục tài liệu chức năng tương ứng trong [`docs/`](file:///e:/project_hybrid_lakehouse_crypto/docs/):
     - Cấu trúc lưu trữ & Warehouse: [`docs/1-architecture/`](file:///e:/project_hybrid_lakehouse_crypto/docs/1-architecture/)
     - Phân tích nguyên nhân gốc rễ, benchmark & RCA: [`docs/2-postmortems-and-tuning/`](file:///e:/project_hybrid_lakehouse_crypto/docs/2-postmortems-and-tuning/)
     - Bản đặc tả kỹ thuật: [`docs/3-specifications/`](file:///e:/project_hybrid_lakehouse_crypto/docs/3-specifications/)
     - Quy trình vận hành & runbook: [`docs/4-runbooks/`](file:///e:/project_hybrid_lakehouse_crypto/docs/4-runbooks/)
     - Data contract & chuẩn hóa dữ liệu: [`docs/5-data-contracts/`](file:///e:/project_hybrid_lakehouse_crypto/docs/5-data-contracts/)
  3. [`README.md`](file:///e:/project_hybrid_lakehouse_crypto/README.md):
     - Cập nhật mục lục tài liệu nếu có tài liệu mới phát sinh.

### 1.1. Quy trình ghi nhận khi Phát hiện / Sửa một Lỗi Mới (Bug & Incident Logging)
Khi phát hiện lỗi hoặc khắc phục một bug hệ thống, Agent **bắt buộc ghi nhận Post-Mortem** vào file [`docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md) theo mẫu:
- **Triệu chứng (Symptom)**: Mô tả hiện tượng lỗi, thông báo exception / log trace.
- **Nguyên nhân gốc rễ (Root Cause Analysis - RCA)**: Phân tích cơ chế kỹ thuật gây ra lỗi (ví dụ tràn bộ nhớ JVM, lỗi phân vùng, trôi khóa chính).
- **Giải pháp xử lý (Resolution)**: Mô tả các thay đổi mã nguồn, cơ chế xử lý ngoại lệ hoặc tái cấu trúc đã áp dụng.
- **Biện pháp phòng ngừa (Prevention)**: Đưa ra invariant hoặc test case bổ sung để lỗi không tái diễn.
- *Cập nhật 1 dòng tóm tắt vào `MEMORY.md`*.

### 1.2. Quy trình ghi nhận khi Tối ưu hóa Hiệu năng (Performance Optimization Logging)
Khi tối ưu thành công một luồng xử lý hoặc pipeline, Agent **bắt buộc ghi nhận đo đạc định lượng** vào [`docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md`](file:///e:/project_hybrid_lakehouse_crypto/docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md):
- **Điểm nghẽn cũ (Bottleneck)**: Nêu rõ hạn chế (CPU, Disk I/O, JVM Heap, Small files).
- **Kiến trúc tối ưu mới**: Trình bày giải pháp thay thế (ví dụ chuyển sang PyIceberg streaming, DuckDB vectorization).
- **Bảng đo lường đối chiếu (Before vs After)**:
  - Mức sử dụng RAM (Peak RAM Usage)
  - Tốc độ xử lý (Rows/second)
  - Tổng thời gian hoàn thành (Wall-clock time)
- *Cập nhật số liệu mới nhất vào bảng tổng kết trong `README.md` và `MEMORY.md`*.

---

## 2. Single Source of Truth for DDL & SQL Views (Nguồn chân lý duy nhất cho DDL)
- Airflow DAGs (`lakehouse_historical_backfill` và `lakehouse_incremental_hourly`) trực tiếp thực thi file:
  [`sql/ddl/02_create_gold_views.sql`](file:///e:/project_hybrid_lakehouse_crypto/sql/ddl/02_create_gold_views.sql)
- **Quy tắc bắt buộc**:
  - Khi bổ sung view phân tích mới (ví dụ: `v_fiat_stablecoin_rates`, `v_daily_cross_exchange_normalized_spread`), phải ghi trực tiếp vào file [`sql/ddl/02_create_gold_views.sql`](file:///e:/project_hybrid_lakehouse_crypto/sql/ddl/02_create_gold_views.sql).
  - Tuyệt đối không tạo các file SQL đơn lẻ rời rạc bên ngoài thư mục chuẩn vì sẽ làm mất tính đồng bộ với Airflow orchestration.

---

## 3. Link Integrity & Directory Conventions (Quy chuẩn cấu trúc thư mục)
- **Đường dẫn tương đối**: Mọi liên kết trong tài liệu Markdown phải sử dụng đường dẫn tương đối chính xác.
- **Vệ sinh thư mục gốc (Root Hygiene)**:
  - Thư mục gốc (`root`) chỉ được phép chứa: `README.md`, `MEMORY.md`, và các cấu hình cơ sở (`.env.example`, `docker-compose.yml`, `.gitignore`).
  - Toàn bộ tài liệu giải thích, kiến trúc, spec phải nằm gọn gàng trong các thư mục con tương ứng của `docs/`.

---

## 4. Preservation of Technical Depth (Bảo toàn chiều sâu kỹ thuật)
- Khi tóm tắt hoặc viết lại tài liệu, tuyệt đối không lược bỏ các chi tiết kỹ thuật chuyên sâu:
  - Các công thức toán học KaTeX (RCA OOM memory calculation, Arbitrage basis).
  - Bảng số liệu benchmark thực nghiệm (tốc độ rows/second, thông số RAM heap, số lượng partitions).
  - Phân tích rủi ro tài chính chuyên sâu (De-peg risk giữa USDT, USDC và USD).
