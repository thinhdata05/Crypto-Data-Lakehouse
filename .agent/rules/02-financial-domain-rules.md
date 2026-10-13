---
description: Financial market conventions, quote currency preservation, stablecoin peg normalization, and OHLCV sanity rules
globs: ["src/preprocessing/**", "src/spark/**", "sql/**", "src/incremental/**"]
---

# 02. Financial Domain & Currency Standardization Rules

Quy tắc nghiệp vụ thị trường tài chính, xử lý đồng yết giá, rủi ro biến động tỷ giá Stablecoin và chuẩn hóa dữ liệu OHLCV.

---

## 1. Multi-Exchange Liquidity & Pricing Independence
- **Alpaca Crypto**:
  - Đồng tiền định giá gốc là Fiat chuẩn (`USD`).
  - Thanh khoản và sổ lệnh tuân thủ quy chuẩn thị trường tài chính Hoa Kỳ.
- **Binance**:
  - Đồng tiền định giá chủ đạo là Stablecoin (`USDT`, `USDC`).
  - Thanh khoản và độ sâu thị trường thuộc mạng lưới giao dịch tiền mã hóa toàn cầu.
- **Quy tắc phân tích giá**:
  - Giá của cùng một tài sản cơ sở (ví dụ `BTC`, `ETH`) trên Alpaca và Binance có thể chênh lệch tự nhiên do phí giao dịch, cơ chế khớp lệnh và biến động thanh khoản tức thời.
  - Tuyệt đối không được xem độ lệch giá giữa 2 sàn là "lỗi dữ liệu" và không được áp đặt giá của sàn này để hiệu chỉnh sàn kia trong các bảng cơ sở.

---

## 2. Quote Currency Preservation in Base Storage (Nguyên tắc bảo toàn kiểm toán)
- Trong toàn bộ các bảng lưu trữ vật lý của Medallion Architecture:
  - `lakehouse.bronze.crypto_bars`
  - `lakehouse.silver.crypto_bars`
  - `lakehouse.silver.curated_crypto_bars`
  - `lakehouse.gold.curated_crypto_bars`
- **QUY TẮC BẮT BUỘC**:
  - Lưu trữ giá `open`, `high`, `low`, `close` đúng theo đơn vị tiền tệ định giá gốc (`quote_currency`): Alpaca lưu theo `USD`, Binance lưu theo `USDT` hoặc `USDC`.
  - **TUYỆT ĐỐI CẤM TỰ ĐỘNG NHÂN TỶ GIÁ** để đổi `USDT` thành `USD` trong các bảng vật lý này.
  - **Lý do kỹ thuật & tài chính**:
    1. Bất kỳ phép quy đổi nào ở tầng cơ sở đều phá hủy dấu vết kiểm toán (Audit Trail) và làm mất tính trung thực của dữ liệu khớp lệnh gốc.
    2. Tỷ giá Stablecoin biến động theo từng phút; nếu nhân tỷ giá tĩnh hoặc tỷ giá xấp xỉ sẽ làm sai lệch vĩnh viễn dữ liệu tài chính lịch sử.

---

## 3. Binance Quote Pruning Hierarchy (Thứ tự ưu tiên thanh khoản)
- Trên Binance, một tài sản cơ sở có thể được niêm yết với nhiều cặp giao dịch (ví dụ: `BTCUSDT`, `BTCUSDC`, `BTCUSD`, `BTCBTC`).
- Nhằm tránh phân mảnh dữ liệu và trùng lặp không cần thiết, bộ tiền xử lý (`src/preprocessing/binance.py`) áp dụng quy tắc lọc ưu tiên thanh khoản:
  $$\mathbf{USDT} > \mathbf{USDC} > \mathbf{USD} > \mathbf{BTC}$$
- Nếu tài sản có cặp với `USDT`, hệ thống chỉ thu thập cặp `USDT` và loại bỏ các cặp còn lại.

---

## 4. Normalized Currency Conversion at Serving Layer (Chuẩn hóa tỷ giá tầng phục vụ)
- Việc quy đổi tiền tệ để tính toán chênh lệch giá (Cross-Exchange Arbitrage Spread) **CHỈ ĐƯỢC PHÉP THỰC HIỆN Ở TẦNG SERVING (VIEW / ANALYTICS)**:
  - **View `lakehouse.gold.v_fiat_stablecoin_rates`**: Cung cấp chuỗi tỷ giá phút/ngày giữa `USDT-USD` và `USDC-USD`.
  - **View `lakehouse.gold.v_daily_cross_exchange_normalized_spread`** (hoặc bounded API `src/serving/query_minute_spread.py` cho cấp phút): Thực hiện chuẩn hóa giá Binance về USD trước khi so sánh với Alpaca:
    $$\text{normalized\_binance\_close} = \text{binance\_close} \times \text{coalesce(usdt\_usd\_rate, 1.0)}$$
    $$\text{normalized\_spread\_usd} = \text{alpaca\_close} - \text{normalized\_binance\_close}$$
    $$\text{normalized\_spread\_bps} = \left(\frac{\text{normalized\_spread\_usd}}{\text{alpaca\_close}}\right) \times 10,000$$
- **Rủi ro De-peg**: Khi xảy ra sự kiện thiên nga đen (ví dụ USDT tụt xuống 0.95 USD hoặc USDC de-peg về 0.88 USD), công thức chuẩn hóa này phản ánh chính xác cơ hội kinh doanh chênh lệch giá thực tế, thay vì tạo ra tín hiệu giả mạo do giả định sai $1\text{ USDT} = 1\text{ USD}$.

---

## 5. OHLCV Data Quality & Sanity Rules
- Mỗi thanh nến 1 phút (1-minute bar) khi nạp vào Silver bắt buộc phải vượt qua bộ kiểm tra tính nhất quán toán học OHLCV:
  1. $\text{high} \ge \text{low}$
  2. $\text{high} \ge \text{open}$
  3. $\text{high} \ge \text{close}$
  4. $\text{low} \le \text{open}$
  5. $\text{low} \le \text{close}$
  6. $\text{volume} \ge 0$
  7. $\text{open} > 0 \land \text{high} > 0 \land \text{low} > 0 \land \text{close} > 0$
- **Hành động**: Các bản ghi không đạt kiểm tra này sẽ bị đánh dấu cờ lỗi `is_valid_sanity = false` tại Silver và **BỊ CHẶN TUYỆT ĐỐI** không được nạp vào `gold.curated_crypto_bars`.

---

## 6. Temporal Integrity & Nghiêm cấm Synthetic Rows
- Dữ liệu lịch sử của mỗi symbol bắt đầu chính xác từ thời điểm sàn niêm yết (listing date) trong khoảng $\ge 2021-01-01$ UTC.
- **Nghiêm cấm tuyệt đối**:
  - Không tự sinh các dòng giả mạo (synthetic / dummy bars) hoặc forward-fill để lấp đầy những khoảng thời gian trước khi tài sản được niêm yết.
  - Những phút không có giao dịch (zero trading volume / missing minutes) phải được giữ nguyên trạng thái trống tự nhiên của sổ lệnh.
