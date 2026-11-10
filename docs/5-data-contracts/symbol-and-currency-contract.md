# Hợp Đồng Dữ Liệu (Data Contract): Danh Mục Symbol, Schema Chuẩn & Quy Tắc Tiền Tệ

> **Nền tảng**: Multi-Exchange Crypto Lakehouse Platform (`s3://lakehouse/`)  
> **Cơ chế thực thi**: Kiểm định Schema & Nghiệp vụ bằng PyArrow, DuckDB, PyIceberg và Iceberg DDL  
> **Phạm vi**: 68 Cặp Giao Dịch Chuẩn Hóa trên Alpaca (34) và Binance (34) giai đoạn 2021 – 2026

---

## 1. Các Nguyên Tắc Bất Biến Cốt Lõi (Core Data Invariants)

### A. Khóa Chính Hợp Nhất (Primary Business Key)
Khóa định danh duy nhất trên toàn bộ các tầng dữ liệu là bộ 3 trường:
$$\text{Business Key} = (\text{exchange}, \text{raw\_symbol}, \text{timestamp})$$

- **Độc lập đa sàn (Multi-Exchange Independence)**:
  - Alpaca và Binance vận hành sổ lệnh (order book), bể thanh khoản và bộ khớp lệnh độc lập hoàn toàn.
  - Bản ghi của `BTC_USD` trên Alpaca và `BTCUSDT` trên Binance tại cùng một phút **KHÔNG BAO GIỜ là bản ghi trùng lặp** và **TUYỆT ĐỐI KHÔNG ĐƯỢC ghi đè lên nhau**.
- **Giải quyết xung đột ưu tiên nội bộ sàn ($3 > 2 > 1$)**:
  - Quy tắc ưu tiên ($3 > 2 > 1$, phân xử hòa bằng `ingested_at DESC`) **chỉ áp dụng trong cùng một sàn (`exchange`)**:
    - `priority = 3` (`source = 'correction'`): Bản ghi đính chính trễ từ sàn qua WebSocket (`T='u'`) hoặc hiệu chỉnh kiểm toán.
    - `priority = 2` (`source = 'historical'`): Dữ liệu lịch sử (Historical Backfill) và dữ liệu REST API hàng giờ (Incremental REST) đã đóng nến.
    - `priority = 1` (`source = 'realtime'`): Nến thời gian thực sơ bộ qua WebSocket (`T='b'`).
- **Nghiêm cấm sinh dòng giả (Zero Synthetic Rows)**:
  - Dữ liệu của mỗi mã tài sản chỉ bắt đầu từ thời điểm niêm yết thực tế trên sàn đó ($\ge 2021-01-01$ UTC).
  - Tuyệt đối không tự tạo dòng giả (dummy bars) hoặc điền giá trị (forward-fill) cho các phút không có giao dịch.

---

## 2. Cấu Trúc Bảng Chuẩn Hóa (Conformed Schemas)

### A. Bảng Dữ Liệu Tầng Bronze (`lakehouse.bronze.crypto_bars` — Local)
Bảng lưu trữ bất biến (Immutable, Append-Only), phân vùng theo `PARTITIONED BY (days(timestamp))`:

| Tên Cột | Kiểu Dữ Liệu | Nullable | Mô Tả | Ví Dụ |
| :--- | :--- | :---: | :--- | :--- |
| `exchange` | `STRING` | NO | Định danh sàn giao dịch (`'alpaca'`, `'binance'`) | `'binance'` |
| `raw_symbol` | `STRING` | NO | Mã cặp giao dịch gốc của sàn (Alpaca dùng `_`, Binance viết liền) | `'BTCUSDT'` / `'BTC_USD'` |
| `timestamp` | `TIMESTAMPTZ` | NO | Thời điểm mở nến 1 phút theo giờ chuẩn UTC | `'2024-09-04 12:00:00+00:00'` |
| `open` | `DOUBLE` | NO | Giá mở cửa của nến 1 phút | `58250.00` |
| `high` | `DOUBLE` | NO | Giá cao nhất trong phút | `58320.50` |
| `low` | `DOUBLE` | NO | Giá thấp nhất trong phút | `58210.10` |
| `close` | `DOUBLE` | NO | Giá đóng cửa của nến 1 phút | `58290.00` |
| `volume` | `DOUBLE` | NO | Khối lượng tài sản cơ sở khớp lệnh trong phút | `42.158` |
| `trade_count` | `BIGINT` | NO | Số lượng lệnh khớp trong phút | `1842` |
| `source` | `STRING` | NO | Nguồn dữ liệu (`'historical'`, `'realtime'`, `'correction'`) | `'historical'` |
| `base_currency` | `STRING` | NO | Mã tài sản cơ sở (Base Asset) | `'BTC'` |
| `quote_currency`| `STRING` | NO | Đồng tiền định giá gốc (giữ nguyên bản, không quy đổi) | `'USDT'` |
| `canonical_symbol`| `STRING` | NO | Cặp chuẩn hóa có dấu gạch ngang (`Base-Quote`) | `'BTC-USDT'` |
| `batch_id` | `STRING` | NO | Mã UUID của lô nạp dữ liệu | `'28c4df2d-...'` |
| `ingested_at` | `TIMESTAMPTZ` | NO | Thời điểm bản ghi được ghi vào Iceberg Bronze | `'2026-11-08 02:29:35+00:00'` |

### B. Bảng Dữ Liệu Tầng Silver (`lakehouse.silver.bars` — Hợp Nhất Trên Local & Cloud)
Bảng dữ liệu chuẩn duy nhất tại tầng Silver (`PARTITIONED BY (days(timestamp))`), bổ sung cột `priority` và cam kết 100% không trùng khóa `(exchange, raw_symbol, timestamp)`:

| Tên Cột | Kiểu Dữ Liệu | Nullable | Mô Tả | Ví Dụ |
| :--- | :--- | :---: | :--- | :--- |
| *(13 cột đầu)* | *(Giống Bronze)* | NO | Đã vượt qua 7 quy tắc kiểm định toán học OHLCV | — |
| `priority` | `INT` | NO | Trọng số ưu tiên ($3 > 2 > 1$) để giải quyết xung đột nội bộ sàn | `2` |
| `batch_id` | `STRING` | NO | Mã UUID của lô dữ liệu chiến thắng sau khi khử trùng lặp | `'28c4df2d-...'` |
| `ingested_at` | `TIMESTAMPTZ` | NO | Mốc thời gian dùng để phân xử khi cùng mức `priority` (`DESC`) | `'2026-11-08 05:08:09+00:00'` |

---

## 3. Danh Mục 68 Cặp Tài Sản Chuẩn Hóa & Cơ Sở Lý Thuyết Vi Mô Thị Trường

Hệ thống theo dõi chính xác **34 tài sản tiền mã hóa cốt lõi** trên cả hai sàn (**34 cặp trên Alpaca + 34 cặp trên Binance = 68 symbols**).

### A. Thứ Tự Ưu Tiên Chọn Đồng Tiền Định Giá (Quote Currency Pruning Hierarchy)
Để tránh phân mảnh thanh khoản và bùng nổ tổ hợp khi join chéo giữa hai sàn, bộ tiền xử lý (`src/preprocessing/alpaca.py`, `src/preprocessing/binance.py`) chọn **1 đồng tiền định giá có thanh khoản cao nhất cho mỗi tài sản trên mỗi sàn**:
$$\text{Binance}: \mathbf{USDT} > \mathbf{USDC} > \mathbf{USD} > \mathbf{BTC} \quad\mid\quad \text{Alpaca}: \mathbf{USD} > \mathbf{USDT} > \mathbf{USDC}$$

- **Phân bổ trên Alpaca (34 symbols)**:
  - **23 cặp định giá bằng `USD`**: `AAVE_USD`, `ADA_USD`, `ARB_USD`, `AVAX_USD`, `BAT_USD`, `BCH_USD`, `BONK_USD`, `BTC_USD`, `DOGE_USD`, `ETC_USD`, `ETH_USD`, `FIL_USD`, `HBAR_USD`, `ICP_USD`, `INJ_USD`, `LTC_USD`, `NEAR_USD`, `OP_USD`, `PEPE_USD`, `RENDER_USD`, `SOL_USD`, `TRUMP_USD`, `XRP_USD`.
  - **7 cặp định giá bằng `USDT`**: `APT_USDT`, `ATOM_USDT`, `FET_USDT`, `LINK_USDT`, `SHIB_USDT`, `SUSHI_USDT`, `UNI_USDT` *(hoặc `YFI_USDT` tùy lịch sử niêm yết)*.
  - **4 cặp định giá bằng `USDC`**: `CRV_USDC`, `DOT_USDC`, `GRT_USDC`, `XTZ_USDC`.
- **Phân bổ trên Binance (34 symbols)**:
  - **28 cặp định giá bằng `USDT`**: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `ADAUSDT`, `AVAXUSDT`, ...
  - **4 cặp định giá bằng `USDC`**: `CRVUSDC`, `DOTUSDC`, `GRTUSDC`, `XTZUSDC`.
  - **1 cặp định giá bằng `USD`**: `USDCUSD`.
  - **1 cặp định giá bằng `TUSD`**: `USDTUSD`.

### B. Chứng Minh Định Lượng: Price Discovery (Hasbrouck Information Share & Granger Causality)
1. **Bất đối xứng sổ lệnh (Order Book Asymmetry)**:
   - Trên mỗi sàn, cặp giao dịch chính (ví dụ `BTC/USD` trên Alpaca hoặc `BTCUSDT` trên Binance) tập trung $80\% - 95\%$ tổng khối lượng giao dịch và độ sâu sổ lệnh với chênh lệch giá mua-bán (bid-ask spread) cực hẹp ($< 2 \text{ bps}$).
   - Các cặp phụ trên cùng sàn chỉ chiếm $< 15\%$ thanh khoản và có spread giãn rộng ($> 15 - 30 \text{ bps}$).
2. **Kiểm định nhân quả Granger (Granger Causality)**:
   $$\Delta \ln(P_{\text{primary}, t}) \xrightarrow{\text{Granger}} \Delta \ln(P_{\text{secondary}, t+1}) \quad (p < 0.001)$$
   Biến động giá phát sinh từ sổ lệnh chính dẫn dắt sổ lệnh phụ với độ trễ dưới 1 giây, trong khi chiều ngược lại không có ý nghĩa thống kê ($p > 0.05$). Các nhà tạo lập thị trường (Market Makers) trên cặp phụ thực chất chỉ neo giá theo cặp chính:
   $$\text{Secondary Mid Quote}_t = \text{Primary Mid Quote}_t \times \text{FX Rate}_{\text{Quote}_{\text{primary}} / \text{Quote}_{\text{secondary}}, t}$$
3. **Ngăn chặn bùng nổ ma trận tổ hợp ($M \times N$)**:
   - Việc giữ 1 cặp đại diện thanh khoản cao nhất cho mỗi tài sản trên mỗi sàn giúp phép nối liên sàn (Cross-Exchange Join) đạt tỷ lệ 1:1 sạch tuyệt đối, loại bỏ 90% nhiễu do trượt giá bid-ask trên các cặp kém thanh khoản.

---

## 4. Hợp Đồng Bảo Toàn Đồng Tiền Gốc & Chuẩn Hóa Tỷ Giá Tầng Phục Vụ

1. **Nguyên tắc bất tương đương tiền tệ**:
   $$\text{USD (Fiat)} \ne \text{USDT (Tether)} \ne \text{USDC (Circle)}$$
   Các bảng vật lý (`bronze.crypto_bars`, `bronze.rt_bars`, `silver.bars`, `gold.agg_daily_crypto_ohlcv`) **tuyệt đối không tự ý nhân tỷ giá** để đổi `USDT` sang `USD`.
2. **Công thức chuẩn hóa tại Tầng Phục Vụ (`v_daily_cross_exchange_normalized_spread` & `query_minute_spread.py`)**:
   $$\text{Adjusted Binance Price (USD)} = \text{Close}_{\text{Binance}} \times \text{coalesce}(\text{Rate}_{\text{USDT/USD}}, 1.0)$$
   $$\text{Normalized Spread (USD)} = \text{Close}_{\text{Alpaca (USD)}} - \text{Adjusted Binance Price (USD)}$$
   $$\text{Normalized Spread (\%)} = \left(\frac{\text{Normalized Spread (USD)}}{\text{Adjusted Binance Price (USD)}}\right) \times 100$$

---

## 5. Hợp Đồng Xuất Bản Dữ Liệu Giữa Local và Cloud (`confirmed/` & `_manifest.json`)

Theo kiến trúc **Phase 2 Hybrid (Option 1)**:
1. Local **không bao giờ mở giao dịch ghi trực tiếp vào Cloud Iceberg Catalog** nhằm loại bỏ hoàn toàn xung đột khóa (Catalog Lock Contention) qua mạng WAN.
2. Sau mỗi giờ chạy `incremental_sync.py` vào Local `silver.bars`, module [`publish_confirmed_parquet.py`](../../src/pipeline/incremental/publish_confirmed_parquet.py) xuất bản:
   - Các file Parquet bất biến theo từng sàn (`alpaca_bars.parquet`, `binance_bars.parquet`) với `priority = 2`.
   - File commit nguyên tử `_manifest.json` được ghi **cuối cùng**, chứa đầy đủ `batch_id`, `window_start`, `window_end`, `total_records`, và danh sách `files[]` kèm mã kiểm tra toàn vẹn `sha256`.
3. Trong kiến trúc Cloud đề xuất, dịch vụ Single Writer (AWS EMR Serverless / Glue) sẽ kiểm tra mã băm `sha256` trong `_manifest.json` trước khi hợp nhất với `bronze.rt_bars` (`priority IN (1, 3)`) vào bảng `silver.bars` trên Cloud.
