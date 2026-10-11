# AGENTS.md: AI Agent Operational Instructions & Lakehouse Guidelines

Chào mừng AI Agent tham gia làm việc trên dự án **Multi-Exchange Crypto Lakehouse Platform**.
Trước khi thực hiện bất kỳ hành động nào, bạn **BẮT BUỘC** phải đọc và tuân thủ nghiêm ngặt các tài liệu và quy tắc vận hành sau:

---

## 1. Quick Navigation & Context
- [**`MEMORY.md`**](file:///e:/project_hybrid_lakehouse_crypto/MEMORY.md): Bản ghi trạng thái hiện tại của Lakehouse, 100.6M records, 68 symbols, kết quả kiểm toán, và Roadmap Phase 2.
- [**`README.md`**](file:///e:/project_hybrid_lakehouse_crypto/README.md): Hướng dẫn kiến trúc, thiết kế Medallion, mô tả các bảng, và lệnh khởi động nhanh.

---

## 2. Agent Operational Rules Index (`.agent/rules/`)
Tất cả các quy tắc vận hành cốt lõi được định nghĩa chi tiết tại thư mục [`.agent/rules/`](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/):

1. [**`01-lakehouse-invariants.md`**](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/01-lakehouse-invariants.md):
   - **Business Key**: `(exchange, raw_symbol, timestamp)`
   - **Multi-Exchange Independence**: Không bao giờ gộp hay ghi đè dữ liệu giữa Alpaca và Binance.
   - **Intra-Exchange Priority**: Mức ưu tiên $3 > 2 > 1$ chỉ áp dụng trong cùng 1 sàn.
   - **Partitioning**: Bắt buộc `days(timestamp)`, cấm phân vùng theo symbol.
   - **Deduplication & MERGE INTO**: Chuẩn hóa logic cập nhật dữ liệu.
   - **Zero-JVM-OOM Architecture**: PyIceberg streaming + DuckDB in-process.

2. [**`02-financial-domain-rules.md`**](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/02-financial-domain-rules.md):
   - **Quote Currency Preservation**: Tuyệt đối không quy đổi USDT sang USD trong các bảng vật lý cơ sở.
   - **Binance Hierarchy**: Lọc ưu tiên `USDT > USDC > USD > BTC`.
   - **Normalized Spread Serving**: Quy đổi chuẩn hóa tỷ giá chỉ thực hiện ở View (`v_cross_exchange_normalized_spread`) qua bảng tỷ giá (`v_fiat_stablecoin_rates`).
   - **OHLCV Sanity**: 100% tuân thủ bất đẳng thức OHLCV và volume $\ge 0$.
   - **No Synthetic Rows**: Bắt đầu dữ liệu từ ngày niêm yết thực tế ($\ge 2021-01-01$), không sinh dòng giả.

3. [**`03-safety-and-devops.md`**](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/03-safety-and-devops.md):
   - **Docker Containers**: Chạy Spark, Airflow bên trong Docker containers.
   - **JVM Protection**: Luôn thêm điều kiện `WHERE timestamp BETWEEN ... AND ...` khi truy vấn trên 100M dòng để tránh sập JVM thread pool.
   - **Storage Safety**: Bảo vệ xô MinIO `s3://warehouse/` và 2 bảng benchmark `test_spark_clustered`, `test_spark_range`.
   - **Idempotency**: Mọi pipeline phải an toàn khi chạy lại nhiều lần.
   - **Secrets**: Mọi thông tin mật đọc từ `.env`.

4. [**`04-documentation-rules.md`**](file:///e:/project_hybrid_lakehouse_crypto/.agent/rules/04-documentation-rules.md):
   - **Auto-Documentation Reflex**: Bắt buộc tự động cập nhật tài liệu khi code/schema thay đổi.
   - **Bug & Incident Logging**: Mọi lỗi mới hoặc khắc phục bug phải ghi lại Post-Mortem (Symptom, RCA, Resolution, Prevention) vào `docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md` và tóm tắt vào `MEMORY.md`.
   - **Performance Optimization Logging**: Mọi tối ưu hóa phải ghi nhận số liệu đo lường Before vs After (Peak RAM, Rows/sec, Time) vào `docs/2-postmortems-and-tuning/root-cause-and-benchmarks.md` và cập nhật benchmark trong `README.md`.
   - **Single Source of Truth**: Thêm Views mới trực tiếp vào `sql/ddl/02_create_gold_views.sql`.
   - **Preservation of Technical Depth**: Giữ gìn công thức toán KaTeX và số liệu benchmark thực tế.
