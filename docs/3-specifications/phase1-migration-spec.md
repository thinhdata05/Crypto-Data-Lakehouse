# PROJECT LAKEHOUSE CRYPTO

# Architecture & Migration Specification for Agent

# Target workspace: E:\project_hybrid_lakehouse_crypto

# Reference workspace: E:\project_realtime_crypto

============================================================
0. MỤC ĐÍCH
==============

Project mới E:\project_hybrid_lakehouse_crypto là phiên bản được thiết kế lại
dựa trên project đã hoàn thành tại:

E:\project_realtime_crypto

Agent phải:

1. Đọc và hiểu project cũ trước.
2. Kiểm tra các DAG, Spark jobs, SQL, Docker Compose, configuration,
   data flow và các logic xử lý dữ liệu đã tồn tại.
3. Không copy mù toàn bộ implementation cũ.
4. Tái sử dụng các logic đúng, đồng thời tổ chức lại kiến trúc
   phù hợp với thiết kế mới.
5. Không phá vỡ các nguyên tắc về data quality, incremental processing,
   merge và priority.
6. Phase 1 phải chạy được hoàn toàn trên local Docker.
7. Phase 2 mới mở rộng phần realtime lên AWS.

Project này ưu tiên:

- Data Engineering
- Lakehouse architecture
- Historical backfill
- Incremental processing
- Data reconciliation
- Priority-based merge (trong nội bộ một exchange)
- Multi-exchange data modeling (Alpaca + Binance, KHÔNG gộp thành 1 nguồn)
- Cloud migration
- Khả năng mở rộng sang ML/MLOps trong tương lai

============================================================

1. REFERENCE PROJECT
   ============================================================

Project cũ:

E:\project_realtime_crypto

Agent PHẢI kiểm tra project cũ trước khi implement.

Các thành phần cần kiểm tra:

- docker-compose.yml
- airflow/
- src/
- src/spark/
- sql/
- config/
- tests/
- docs/
- README.md
- các DAG historical backfill
- DAG incremental hourly
- Spark jobs (đặc biệt: stream_kafka_to_bronze.py, silver_transform.py,
  gold_transform.py — logic MERGE/foreachBatch/watermark có thể tái sử
  dụng cho phần Silver->Curated Silver->Gold ở project mới, dù đổi từ
  Delta Lake sang Iceberg)
- monitoring/audit logic
- Delta Lake/Hive Metastore logic nếu còn tồn tại
- PostgreSQL usage
- MinIO configuration

Mục tiêu của việc kiểm tra:

Xác định:

- logic nào đã đúng
- logic nào cần giữ
- logic nào cần refactor
- logic nào chỉ phục vụ project cũ
- logic nào không cần đưa sang project mới

LƯU Ý QUAN TRỌNG: project cũ dùng merge key (symbol, timestamp) và chỉ
có 1 exchange (Alpaca). Project mới có 2 exchange (Alpaca + Binance) nên
merge key BẮT BUỘC phải mở rộng thêm exchange (xem mục 14). Đây là điểm
khác biệt cốt lõi nhất giữa 2 project, KHÔNG được copy nguyên merge key
cũ sang.

============================================================
2. KIẾN TRÚC MỤC TIÊU - PHASE 1
===================================

Phase 1 chạy LOCAL bằng Docker. Phase 1 CHỈ xử lý historical +
incremental (REST API theo lịch). Phase 1 KHÔNG có streaming/WebSocket
thật (xem mục 11).

Technology stack:

Airflow (orchestration)
    ↓
Python (source preprocessing, theo từng exchange)
    ↓
Raw (đã chuẩn hóa schema, chưa merge)
    ↓
Spark (Bronze → Silver → Curated Silver → Gold)
    ↓
Apache Iceberg (table format)
    ↓
MinIO (local object storage)

Cụ thể:

Python:

- đọc file raw đã tải sẵn (Alpaca .parquet, Binance .parquet)
- preprocessing THEO TỪNG EXCHANGE riêng biệt (không trộn logic)
- chuẩn hóa tên cột về schema chung
- chuẩn hóa timestamp về UTC
- chuẩn hóa datatype (float64 cho giá, int64 cho trade_count)
- tách symbol thành base_currency + quote_currency
- gắn cột exchange, source = "historical"
- validate dữ liệu (loại record thiếu giá, volume âm, high < low)
- KHÔNG gán priority ở bước này (priority là business rule, xử lý ở
  Spark/Silver, không phải việc của preprocessing)

Airflow:

- orchestration
- historical backfill (đọc file raw đã có sẵn local, KHÔNG gọi API)
- incremental processing (gọi REST API Alpaca + Binance, lấy phần dữ
  liệu mới kể từ lần backfill/incremental gần nhất)
- scheduling, retry, dependency management
- monitoring/audit

Spark:

- xử lý distributed data
- Raw → Bronze (ghi gần như nguyên trạng, thêm metadata kỹ thuật)
- Bronze → Silver theo từng exchange (Silver Alpaca, Silver Binance —
  vẫn là 2 bảng/2 partition riêng biệt, CHƯA merge giữa 2 exchange)
- Silver (per-exchange) → Curated Silver: đây là bước MERGE + PRIORITY
  thật sự, nhưng chỉ resolve trùng lặp TRONG CÙNG 1 exchange (xem mục 13)
- Curated Silver → Gold (aggregate, dimensional model)
- Silver → Gold theo dõi số liệu

Apache Iceberg:

- table format chính, thay thế Delta Lake của project cũ
- schema evolution, partitioning, snapshot/versioning
- MERGE INTO / upsert cho bước Silver → Curated Silver
- quản lý bảng lakehouse qua catalog (xem mục 23)

MinIO:

- local object storage, thay thế S3 trong môi trường local

PostgreSQL:

- metadata/service databases khi cần (Airflow metadata database)
- không dùng PostgreSQL làm data lake chính

============================================================
3. STORAGE LAYER
================

Local Phase 1:

E:\project_hybrid_lakehouse_crypto
└── raw
    ├── alpaca
    │   └── bars
    │       └── symbol=BAT_USD
    │           └── year=2021
    │               └── month=01
    │                   ├── day=01.parquet
    │                   └── ...
    │
    └── binance
        └── kline
            └── symbol=BATUSDT
                └── year=2017
                    └── month=01
                        ├── day=01.parquet
                        ├── day=02.parquet
                        └── ...

Raw file giữ NGUYÊN VĂN tên symbol gốc theo từng sàn (immutable raw
data — không được sửa/chuẩn hóa ngay tại tầng raw). Việc chuẩn hóa
diễn ra ở bước preprocessing/Bronze, không sửa file raw gốc.

MinIO sẽ là object storage chính của lakehouse trong Phase 1
(Bronze/Silver/Curated Silver/Gold đều lưu trên MinIO qua Iceberg).

============================================================
4. HISTORICAL DATA SOURCES
==========================

Historical Backfill có 2 source, coi là 2 EXCHANGE ĐỘC LẬP:

1. Alpaca (raw_symbol dạng "BASE/QUOTE", VD "BAT/USD")
2. Binance (raw_symbol dạng liền nhau, VD "BATUSDT")

Cả hai source chỉ sử dụng dữ liệu OHLCV làm dữ liệu chính.

Không cần historical trades API ở Phase 1.
Không cần order book ở Phase 1.

QUYẾT ĐỊNH PHẠM VI (SCOPE DECISION): mỗi symbol chỉ giữ 1 quote
currency duy nhất khi ingest (VD ưu tiên USD cho Alpaca, USDT cho
Binance) để giảm độ phức tạp ở Phase 1. Không bắt buộc ingest tất cả
các cặp quote khác nhau (VD bỏ qua BAT/USDC nếu đã có BAT/USD).

============================================================
5. ALPACA HISTORICAL DATA
=========================

Alpaca cung cấp dữ liệu dạng bars.

Schema gốc (raw):

timestamp
open
high
low
close
volume
trade_count
vwap

Các field quan trọng giữ lại: timestamp, open, high, low, close,
volume, trade_count.

vwap không phải field bắt buộc trong canonical schema.

============================================================
6. BINANCE HISTORICAL DATA
==========================

Binance cung cấp dữ liệu kline/candlestick.

Schema gốc (raw):

symbol
timestamp
open
high
low
close
volume
quote_volume
trade_count
taker_buy_base_volume
taker_buy_quote_volume

Canonical fields cần giữ: symbol, timestamp, open, high, low, close,
volume, trade_count.

Các field Binance-specific (quote_volume, taker_buy_base_volume,
taker_buy_quote_volume) KHÔNG bắt buộc giữ trong canonical dataset
Phase 1, nhưng có thể giữ nguyên ở Bronze nếu cần audit/mở rộng sau.

============================================================
7. CANONICAL SCHEMA (SILVER — PER EXCHANGE)
============================================

Đây là schema áp dụng cho Silver của TỪNG exchange (Silver Alpaca,
Silver Binance), TRƯỚC khi merge thành Curated Silver.

Business key (xem mục 14):
    exchange            "alpaca" | "binance"
    raw_symbol          symbol nguyên bản theo đúng format từng sàn
                         (VD "BAT/USD" hoặc "BATUSDT")
    timestamp           UTC, đã chuẩn hóa

Trạng thái dữ liệu:
    source              "historical" | "realtime"
                         (Phase 1: luôn là "historical")
    priority             int, xem mục 12-13

Metadata chuẩn hóa (derived, phục vụ query/analytics xuyên exchange):
    canonical_symbol    VD "BAT-USD", "BAT-USDT" — ghép từ base+quote,
                         KHÔNG dùng để merge, chỉ để dễ query/so sánh
    base_currency       VD "BAT"
    quote_currency      VD "USD", "USDT" — GIỮ NGUYÊN, không quy đổi
                         USDT/USDC/USD về chung 1 giá trị (chúng có
                         thể lệch giá thật, không phải lỗi cần "sửa")

Đo lường (measures):
    open, high, low, close     float64
    volume                     float64
    trade_count                int64

Lineage:
    ingested_at         thời điểm Spark ghi record này vào Silver,
                         dùng để chống ghi đè ngược khi backfill chạy
                         trễ hơn dữ liệu đã có (whenMatchedUpdate chỉ
                         update khi source.ingested_at >= target.ingested_at)

QUY TẮC BẮT BUỘC:

- KHÔNG được merge dữ liệu Alpaca và Binance thành 1 record ở tầng
  Silver này. Mỗi exchange giữ record riêng, kể cả khi canonical_symbol
  trùng nhau (VD "BAT-USD" ở Alpaca và "BAT-USDT" ở Binance là 2 record
  ĐỘC LẬP, không phải duplicate của nhau).
- Việc coi Alpaca và Binance là cùng 1 "market" là quyết định kinh
  doanh (business decision), KHÔNG được agent tự ý quyết định khi code.

============================================================
8. HISTORICAL TIME RANGE
========================

Project thống nhất lấy historical data từ năm 2021 làm MỐC PHÂN TÍCH
CHUNG của project, KHÔNG phải yêu cầu bắt buộc mọi symbol đều có dữ
liệu từ ngày này.

Binance có thể có dữ liệu trước 2021 (một số symbol từ 2017-2018);
Alpaca chỉ có dữ liệu từ 2021. Một số symbol trên Binance được list
sau 2021.

Nguyên tắc:

- Với mỗi (exchange, symbol) riêng biệt, xác định mốc bắt đầu THỰC TẾ
  bằng cách quét file raw đã có, không giả định.
- KHÔNG ép mọi symbol phải có dữ liệu từ 2021-01-01.
- Nếu 1 symbol chưa tồn tại trên 1 exchange tại thời điểm đó, dữ liệu
  của (exchange, symbol) đó bắt đầu từ thời điểm thực tế xuất hiện.
- KHÔNG tạo artificial rows để lấp khoảng thời gian không tồn tại.
- Dữ liệu Binance trước 2021 (nếu đã tải) KHÔNG cần xóa — vẫn giữ ở
  Raw layer (immutable), nhưng pipeline mặc định chỉ xử lý từ 2021 trở
  đi trừ khi có yêu cầu mở rộng project window sau này.

============================================================
9. SOURCE PREPROCESSING
=======================

Preprocessing thực hiện bằng Python, RIÊNG THEO TỪNG EXCHANGE (2 module
preprocessing khác nhau: preprocessing/alpaca.py, preprocessing/binance.py),
KHÔNG dùng chung 1 hàm cho cả 2 vì format symbol/cột khác nhau.

Preprocessing Python chịu trách nhiệm:

- đọc parquet
- kiểm tra schema
- normalize column names về schema chung (mục 7)
- normalize symbol: tách base_currency/quote_currency
  - Alpaca: split theo dấu "/" (VD "BAT/USD" -> "BAT", "USD")
  - Binance: dò từ cuối chuỗi theo danh sách quote currency đã biết
    (USDT, BUSD, USDC...) vì Binance không có ký tự phân cách
- normalize timestamp về UTC
- chuẩn hóa datatype
- gắn exchange, source="historical"
- kiểm tra missing/invalid values (loại record thiếu giá, volume âm,
  high < low)
- KHÔNG gán priority, KHÔNG merge giữa 2 exchange — đây là việc của
  Spark ở bước sau

Spark KHÔNG chịu trách nhiệm cho source-specific preprocessing. Spark
tập trung vào lakehouse processing (Bronze/Silver/Curated Silver/Gold)
sau khi dữ liệu đã có canonical structure.

============================================================
10. DATA FLOW - HISTORICAL
==========================

Alpaca Historical (file raw đã tải sẵn local)
        │
        ▼
   Python preprocessing (preprocessing/alpaca.py)
        │
        ▼
   Raw canonical (schema đã chuẩn hóa, chưa merge)
        │
        ▼
   Spark: Bronze (Alpaca)
        │
        ▼
   Spark: Silver (Alpaca)  ← 1 bảng riêng, exchange="alpaca"

Binance Historical (file raw đã tải sẵn local)
        │
        ▼
   Python preprocessing (preprocessing/binance.py)
        │
        ▼
   Raw canonical (schema đã chuẩn hóa, chưa merge)
        │
        ▼
   Spark: Bronze (Binance)
        │
        ▼
   Spark: Silver (Binance)  ← 1 bảng riêng, exchange="binance"

Silver (Alpaca) + Silver (Binance)
        │
        ▼
   Spark: MERGE + PRIORITY (chỉ resolve trùng TRONG CÙNG exchange,
          xem mục 13) → Curated Silver (union của cả 2 exchange,
          MỖI exchange vẫn là record riêng cho cùng 1 base_currency)
        │
        ▼
   Spark: Curated Gold (aggregate, dimensional model)

============================================================
11. REALTIME DATA - PHASE 1 (CHỈ THIẾT KẾ, CHƯA TRIỂN KHAI)
================================================================

Phase 1 KHÔNG triển khai streaming/WebSocket thật (không thêm Kafka ở
Phase 1 — xem mục 29). Cột "source" trong schema Silver đã dự trù giá
trị "realtime" cho tương lai, nhưng ở Phase 1, mọi record đều có
source="historical".

Việc thiết kế sẵn schema/priority ngay từ Phase 1 (dù chưa dùng tới
giá trị realtime) nhằm mục đích: khi Phase 2 thêm nhánh Alpaca
WebSocket, KHÔNG cần thiết kế lại schema/merge key của Curated Silver.

Realtime source (Phase 2): CHỈ Alpaca (Binance không có nhánh realtime
trong phạm vi project này).

Realtime event có thể có (tham khảo project cũ E:\project_realtime_crypto):

T: "b"  = initial realtime bar
T: "u"  = updated realtime bar (đính chính từ sàn)

============================================================
12. PRIORITY LOGIC
==================

Priority PHẢI được thiết kế ngay từ Phase 1 (dù Phase 1 chỉ có giá trị
priority=2 xuất hiện thực tế, vì chưa có realtime).

QUAN TRỌNG: Priority chỉ áp dụng để resolve trùng lặp GIỮA CÁC TRẠNG
THÁI DỮ LIỆU THEO THỜI GIAN của CÙNG MỘT (exchange, raw_symbol,
timestamp). Priority KHÔNG được dùng để resolve giữa Alpaca và Binance
— đó là 2 exchange độc lập, xem mục 7 và 13.

Priority:

INITIAL_REALTIME_PRIORITY = 1   # bar stream ban đầu, T = "b" (Phase 2)
HISTORICAL_PRIORITY        = 2   # bar từ REST API (Phase 1 luôn dùng giá trị này)
UPDATED_REALTIME_PRIORITY  = 3   # bar đính chính từ sàn, T = "u" (Phase 2)

============================================================
13. PRIORITY RULE (RESOLVE TRONG CÙNG 1 EXCHANGE)
==================================================

Khi cùng một khóa:

(exchange, raw_symbol, timestamp)

xuất hiện ở nhiều trạng thái/lần ghi khác nhau (VD do incremental DAG
chạy nhiều lần, hoặc sau này có cả initial/updated realtime):

chọn record có priority cao hơn (3 > 2 > 1). Nếu priority bằng nhau,
chọn record có ingested_at mới hơn (idempotent nếu bằng nhau).

Ví dụ (Phase 1, chỉ có Alpaca):

exchange=alpaca, raw_symbol=BTC/USD, timestamp=2026-08-08 12:00

Lần backfill đầu:      priority=2, ingested_at=T1
Lần incremental sau:   priority=2, ingested_at=T2 (T2 > T1, cùng
                       priority nhưng dữ liệu correction mới hơn)

→ Final record: giữ bản ingested_at=T2 (theo điều kiện
  whenMatchedUpdateAll(source.ingested_at >= target.ingested_at))

Ví dụ (Phase 2, có đủ 3 trạng thái):

exchange=alpaca, raw_symbol=BTC/USD, timestamp=2026-08-08 12:00

Initial realtime:   priority = 1
Historical:         priority = 2
Updated realtime:   priority = 3

→ Final curated record: priority = 3 (updated realtime thắng)

KHÔNG ÁP DỤNG priority này giữa (exchange=alpaca, BTC/USD, T) và
(exchange=binance, BTCUSDT, T) — đây là 2 record độc lập, cả 2 đều
được giữ lại trong Curated Silver, không có "bản thắng - bản thua".

============================================================
14. MERGE KEY
=============

Canonical business key (Silver per-exchange VÀ Curated Silver):

(exchange, raw_symbol, timestamp)

KHÔNG được dùng row number hoặc ingestion timestamp làm business key.
KHÔNG được dùng (symbol, timestamp) một mình — thiếu exchange sẽ khiến
Alpaca và Binance bị coi là trùng khóa nếu raw_symbol/canonical_symbol
trùng tên, dẫn đến mất dữ liệu khi MERGE.

canonical_symbol, base_currency, quote_currency là METADATA phái sinh,
dùng để query/phân tích xuyên exchange, KHÔNG dùng làm business key,
KHÔNG dùng trong điều kiện MERGE.

============================================================
15. SILVER — 3 KHÁI NIỆM CẦN PHÂN BIỆT
============================================

a) Silver theo từng exchange (Silver Alpaca, Silver Binance):

- Output trực tiếp của Bronze → Silver (per exchange)
- Đã standardized, cleaned, typed, canonicalized THEO TỪNG NGUỒN
- Chưa merge/resolve priority giữa các lần ghi khác nhau của cùng
  1 exchange

b) Curated Silver (hội tụ):

- Output của bước MERGE + PRIORITY áp dụng cho Silver (Alpaca) và
  Silver (Binance) RIÊNG BIỆT (không trộn 2 exchange vào 1 record)
- Đây là nơi resolve trùng lặp trong nội bộ MỖI exchange (theo mục 13)
- Là input cho Curated Gold

c) Realtime Silver (Phase 2, chưa có ở Phase 1):

- Chỉ tồn tại cho exchange=alpaca (WebSocket)
- Ghi liên tục qua Spark Structured Streaming
- Merge nội bộ CHỈ giữa initial (1) và updated (3) của CÙNG exchange
  alpaca — KHÔNG chờ historical (2) để giữ độ trễ thấp
- Đồng thời cũng đóng góp vào Curated Silver (mục b) ở nhịp riêng
  (xem mục 16)

============================================================
16. GOLD ARCHITECTURE
=====================

Project thiết kế 2 loại Gold, KHÁC PHẠM VI MERGE (không phải khác mức
độ merge nhiều/ít):

1. Realtime Gold (Phase 2, chưa có ở Phase 1)

   Realtime Silver (chỉ alpaca, đã merge nội bộ 1 vs 3)
   ↓
   Realtime Gold
   ↓
   near-real-time consumers / future ML inference
   (KHÔNG chờ đối soát với Historical — chấp nhận có thể lệch tạm
   thời nếu sau này Historical/Incremental phát hiện correction)
2. Curated Gold (có từ Phase 1)

   Curated Silver (đã merge đầy đủ theo mục 13, cho MỌI exchange)
   ↓
   Aggregate theo dimensional model (Bronze/Silver KHÔNG aggregate,
   chỉ Curated Silver → Gold mới tổng hợp OHLCV theo kỳ báo cáo)
   ↓
   Curated Gold
   ↓
   BI / Analytics / ML training / Compliance (nguồn authoritative duy
   nhất, KHÔNG dùng Realtime Gold cho mục đích này)

============================================================
17. CURATED GOLD
================

Curated Gold là dữ liệu authoritative/curated, output của Curated
Silver. Mục tiêu:

- phản ánh dữ liệu đã reconcile trong nội bộ từng exchange (priority)
- giữ nguyên tách biệt giữa các exchange (KHÔNG gộp Alpaca/Binance)
- xử lý late data, correction (qua incremental DAG + priority)
- loại bỏ duplicate trong cùng exchange
- tạo dataset ổn định cho downstream

Curated Gold là layer quan trọng nhất để: analytics, reporting, ML
training, compliance, historical analysis.

============================================================
18. PHASE 1 ARCHITECTURE (KHÔNG CÓ REALTIME THẬT)
====================================================

Historical Alpaca (raw local)      Historical Binance (raw local)
        │                                    │
        ▼                                    ▼
Python preprocessing (alpaca.py)   Python preprocessing (binance.py)
        │                                    │
        ▼                                    ▼
    Raw canonical                      Raw canonical
        │                                    │
        ▼                                    ▼
  Spark: Bronze (alpaca)             Spark: Bronze (binance)
        │                                    │
        ▼                                    ▼
  Spark: Silver (alpaca)             Spark: Silver (binance)
        │                                    │
        └──────────────┬─────────────────────┘
                        ▼
        Spark: MERGE + PRIORITY (trong nội bộ mỗi exchange)
                        ▼
                 Curated Silver
                        ▼
                  Curated Gold

Ghi chú: Phase 1 KHÔNG có "Realtime Gold"/"Realtime Silver" thật —
2 khối này chỉ xuất hiện trong kiến trúc Phase 2 (mục 25). Không dựng
job/table rỗng cho Realtime ở Phase 1, chỉ đảm bảo schema (mục 7, 14)
tương thích để thêm vào không phải đổi cấu trúc.

Storage Phase 1:

Local files / Raw canonical
       ↓
MinIO
       ↓
Iceberg tables (Bronze, Silver-per-exchange, Curated Silver, Gold)

============================================================
19. AIRFLOW DAG DESIGN
======================

Phase 1 cần ít nhất 2 DAG:

A. Historical Backfill DAG

Nguồn dữ liệu: ĐỌC FILE RAW ĐÃ TẢI SẴN LOCAL (raw/alpaca/...,
raw/binance/...), KHÔNG gọi REST API trong DAG này.

Nhiệm vụ:

- quét danh sách file raw theo (exchange, symbol, ngày)
- preprocess (Python, theo mục 9)
- Bronze (theo exchange)
- Silver (theo exchange)
- MERGE + PRIORITY → Curated Silver
- Curated Gold
- validation

B. Incremental Hourly DAG

Nguồn dữ liệu: GỌI REST API CỦA CẢ ALPACA VÀ BINANCE, lấy khoảng thời
gian từ mốc đã ingest gần nhất (đọc watermark từ Silver/bảng theo dõi
riêng) tới hiện tại.

Nhiệm vụ:

- với mỗi (exchange, symbol): xác định last_ingested_timestamp
- gọi REST API tương ứng, since=last_ingested_timestamp, until=now
- preprocess (dùng CHUNG module preprocessing/{exchange}.py với backfill)
- gán source="historical", priority=HISTORICAL_PRIORITY (vẫn là 2, vì
  đây vẫn là REST API, không phải WebSocket)
- Bronze → Silver (theo exchange)
- MERGE + PRIORITY → Curated Silver (update các window bị ảnh hưởng)
- Curated Gold (rebuild toàn bộ hoặc theo window ảnh hưởng, tùy volume)
- áp dụng priority nếu có xung đột (whenMatchedUpdateAll theo ingested_at)

============================================================
20. HISTORICAL BACKFILL VS INCREMENTAL
======================================

Historical Backfill:

- load dữ liệu quá khứ ĐÃ TẢI SẴN thành file local
- xử lý nhiều ngày/tháng/năm 1 lần
- có thể chạy lại (idempotent nhờ merge key + priority)
- không gọi API, không tốn quota

Incremental:

- bổ sung dữ liệu MỚI (từ REST API) mà file local backfill không có
- chạy định kỳ (hourly)
- xử lý cửa sổ thời gian nhỏ (từ lần chạy trước tới hiện tại)
- hỗ trợ correction (nếu API trả về data khác với đã ingest, MERGE ghi
  đè theo điều kiện ingested_at)

============================================================
21. KHÔNG ĐƯỢC THIẾT KẾ BACKFILL CỨNG
============================================

Không được assume "Binance luôn có data từ 2017" hoặc "mọi symbol đều
có data từ 2021". Phải kiểm tra availability thực tế bằng cách quét
file raw đang có cho từng (exchange, symbol).

Ví dụ: BTC có data Binance từ 2017, Alpaca từ 2021 — dùng từ 2021 theo
mốc project (mục 8). Một altcoin chỉ được Binance list từ 2023 — dataset
altcoin đó trên Binance bắt đầu từ 2023, KHÔNG tạo dữ liệu giả cho
giai đoạn 2021-2023.

============================================================
22. ICEBERG
===========

Apache Iceberg là table format chính. Không sử dụng Delta Lake trong
architecture mới, trừ khi cần tham khảo implementation cũ để hiểu logic
(watermark, foreachBatch, MERGE pattern) rồi chuyển đổi sang API tương
đương của Iceberg.

Iceberg cần được dùng cho: tables, schema evolution, partitioning,
snapshots, MERGE INTO / upsert, time travel nếu cần.

============================================================
23. CATALOG
===========

Project mới sử dụng Apache Iceberg Catalog.

Agent cần nghiên cứu/đề xuất catalog phù hợp cho local Docker (VD REST
Catalog, Hive Catalog trỏ MinIO, hoặc Nessie).

Mục tiêu:

Local catalog → Iceberg tables → MinIO

Sau này: Iceberg Catalog → S3

Kiến trúc catalog nên được chọn sao cho migration sang AWS (AWS Glue
Data Catalog + Iceberg trên S3) không phải thay đổi toàn bộ table/data
model.

============================================================
24. PHASE 2 - AWS
=================

Phase 2 mới đưa realtime lên AWS.

Realtime source (Alpaca WebSocket)
      ↓
AWS streaming infrastructure (Amazon MSK)
      ↓
Realtime processing (EMR Spark Structured Streaming)
      ↓
Realtime Silver (chỉ exchange=alpaca) → Realtime Gold

Historical/backfill/incremental TIẾP TỤC chạy local Docker (không bắt
buộc chuyển lên AWS ngay — xem mục 26 lý do).

Local (giữ nguyên từ Phase 1):

- Raw, Bronze, Silver (per exchange), Curated Silver, Historical Backfill,
  Incremental DAG

AWS (mới ở Phase 2):

- Realtime ingestion, Realtime processing, Realtime Silver, Realtime Gold

============================================================
25. AWS FUTURE ARCHITECTURE
===========================

Realtime (Alpaca):
AWS MSK → EMR Spark Streaming → Realtime Silver (alpaca) → Realtime Gold
    ↓ (đồng thời ghi song song)
Curated Silver (trên S3, hợp nhất với Historical Silver)

Historical (Alpaca + Binance):
Local Docker → MinIO/Iceberg → Curated Silver → Curated Gold
    ↓
Đẩy Curated Gold (và dần dần Curated Silver) lên Amazon S3 khi kiến
trúc ổn định.

Hội tụ: Curated Silver trên S3 nhận dữ liệu từ CẢ 2 nhánh (Historical
local + Realtime AWS), áp dụng đúng priority rule (mục 12-13) trong
nội bộ exchange=alpaca (vì chỉ alpaca có cả 3 trạng thái 1/2/3).
Exchange=binance chỉ có priority=2 (không đổi so với Phase 1).

============================================================
26. CLOUD STORAGE STRATEGY
==========================

Không nhất thiết phải chuyển toàn bộ Bronze/Silver lên AWS ngay.

Giữ Local: Raw, Bronze, Silver (per exchange), Historical/Incremental
processing.

Chuyển Cloud: Realtime processing, Realtime Gold, Curated Silver/Gold
(điểm phục vụ downstream).

Lý do: (1) giảm AWS cost, (2) historical backfill không cần chạy liên
tục trên cloud, (3) project vẫn chứng minh được cloud architecture qua
nhánh realtime, (4) S3 dùng cho dataset authoritative, (5) chỉ realtime
workload mới thực sự cần cloud scalability.

============================================================
27. FUTURE ML
=============

ML KHÔNG PHẢI requirement của Phase 1. Là future extension.

Use case: Market anomaly detection (phát hiện bất thường đa chiều,
KHÔNG phải rule-based threshold đơn giản kiểu app sàn có sẵn).

Features có thể dùng: OHLC, volume, trade_count, spread (nếu bổ sung
quotes realtime), volatility, rolling statistics, market microstructure
features, order flow imbalance (nếu có buy/sell volume).

Không bắt buộc lấy Trades API ở Phase 1. Phase 1 ưu tiên OHLCV +
trade_count.

============================================================
28. ML FUTURE FLOW
==================

Curated Gold (dữ liệu đã reconcile) → Feature Engineering → Model
Training → Model Artifact
                                            ↓
Realtime Gold (low-latency) → Model Inference (dùng artifact mới nhất)
→ Anomaly Score → Alert

Curated Gold dùng để TRAIN vì dữ liệu đã reconcile (đúng, sạch).
Realtime Gold dùng để INFERENCE vì cần low-latency (chấp nhận có thể
tạm sai nếu sau này Curated phát hiện correction).

============================================================
29. KHÔNG OVER-ENGINEER PHASE 1
================================

Không tự ý thêm ở Phase 1: Kafka (chưa cần vì Phase 1 không có
streaming thật), dbt (Spark đã đủ), MLflow (chưa có ML), Kubernetes,
Feature Store, complex MLOps, order execution, trading bot, order
book, trade-level historical ingestion, EXCHANGE_PRIORITY (không cần
so sánh Alpaca vs Binance — chúng độc lập, xem mục 7/13).

Mục tiêu Phase 1: một lakehouse pipeline hoàn chỉnh, reliable, có
historical + incremental, có merge (trong nội bộ exchange), có
priority, có validation, đúng cho 2 exchange độc lập.

============================================================
30. DEFINITION OF DONE - PHASE 1
================================

[ ] Alpaca historical được ingest từ file raw local.
[ ] Binance historical được ingest từ file raw local.
[ ] Preprocessing tách riêng theo exchange (2 module khác nhau).
[ ] Mỗi symbol chỉ giữ 1 quote currency (theo scope decision mục 4).
[ ] canonical_symbol, base_currency, quote_currency được tính đúng
    cho cả 2 exchange (đặc biệt Binance: tách quote currency từ chuỗi
    liền nhau bằng danh sách quote currency đã biết).
[ ] Historical bắt đầu từ mốc project 2021 theo TỪNG (exchange, symbol),
    không tạo dữ liệu giả cho symbol/giai đoạn chưa tồn tại.
[ ] Airflow historical backfill DAG hoạt động (đọc file local, KHÔNG
    gọi API).
[ ] Airflow incremental DAG hoạt động (gọi REST API Alpaca + Binance).
[ ] Spark xử lý Bronze → Silver (per exchange) → Curated Silver → Gold.
[ ] Iceberg table + Iceberg Catalog hoạt động trên MinIO.
[ ] Silver (per exchange) có đủ schema mục 7 (bao gồm exchange, source,
    canonical_symbol, base_currency, quote_currency, ingested_at).
[ ] Merge key = (exchange, raw_symbol, timestamp) — KHÔNG phải chỉ
    (symbol, timestamp).
[ ] Priority logic hoạt động ĐÚNG PHẠM VI: chỉ resolve trong nội bộ
    1 exchange, KHÔNG áp dụng giữa Alpaca và Binance.
[ ] Curated Silver chứa CẢ record Alpaca lẫn Binance cho cùng 1
    base_currency, KHÔNG gộp thành 1 record.
[ ] Curated Gold reconcile được dữ liệu trong nội bộ từng exchange.
[ ] Có validation/data quality (không tạo duplicate khi chạy lại DAG).
[ ] Có thể giải thích kiến trúc, đặc biệt lý do KHÔNG gộp Alpaca/Binance,
    trong buổi phỏng vấn.

============================================================
31. NGUYÊN TẮC QUAN TRỌNG CHO AGENT
======================================

Trước khi code:

1. Đọc project cũ.
2. Liệt kê architecture hiện tại.
3. Liệt kê các logic có thể tái sử dụng (đặc biệt: MERGE/foreachBatch
   pattern từ silver_transform.py, dù phải chuyển từ Delta sang Iceberg).
4. Liệt kê các logic cần thay đổi (đặc biệt: merge key phải thêm exchange).
5. Kiểm tra source schema thực tế của file raw đã tải (Alpaca, Binance).
6. Đề xuất project structure.
7. Sau đó mới implement.

Không được:

- tự ý đổi merge key về (symbol, timestamp) — PHẢI có exchange
- tự ý bỏ priority
- tự ý bỏ merge
- tự ý MERGE/gộp Alpaca và Binance thành 1 record dù canonical_symbol
  trùng nhau — đây là 2 exchange độc lập
- tự ý tạo dữ liệu thiếu
- tự ý dùng Binance data trước 2021 làm mốc bắt đầu chung của project
  (mốc chung vẫn là 2021, xem mục 8)
- tự ý thêm tool chỉ vì "industry standard" (VD Kafka ở Phase 1)
- tự ý chuyển toàn bộ pipeline lên cloud trong Phase 1
- tự ý thêm EXCHANGE_PRIORITY để so sánh Alpaca vs Binance

============================================================
32. EXPECTED FINAL PROJECT STRUCTURE
====================================

E:\project_hybrid_lakehouse_crypto
├── raw
│   ├── alpaca
│   │   └── bars
│   └── binance
│       └── kline
├── src
│   ├── preprocessing
│   │   ├── alpaca.py
│   │   └── binance.py
│   ├── spark
│   │   ├── bronze
│   │   ├── silver
│   │   ├── curated_silver\      (MERGE + PRIORITY, per exchange)
│   │   └── gold
│   └── ...
├── airflow
│   └── dags
│       ├── historical_backfill_dag.py
│       └── incremental_hourly_dag.py
├── config
├── sql
├── tests
├── docs
├── docker
├── notebooks
├── docker-compose.yml
├── .env
├── .env.example
└── README.md

============================================================
33. FINAL ARCHITECTURE PRINCIPLE
================================

PHASE 1 (local, không có realtime thật):

Historical Alpaca ──► preprocessing/alpaca.py   ──► Bronze ──► Silver(alpaca) ─┐
Historical Binance ─► preprocessing/binance.py  ──► Bronze ──► Silver(binance)─┤
                                                                                ▼
                                                          MERGE + PRIORITY
                                                        (trong nội bộ mỗi exchange)
                                                                                ▼
                                                                       Curated Silver
                                                                                ▼
                                                                        Curated Gold

PHASE 2 (thêm nhánh realtime trên AWS, không đổi nhánh historical):

Historical/Incremental (Alpaca + Binance)
        │  (giữ nguyên chạy Local Docker)
        ▼
 MinIO + Iceberg ──► Curated Silver ──┐
                                       │
Realtime Alpaca ──► AWS MSK ──► EMR   │
   Spark Streaming ──► Realtime       │
   Silver(alpaca) ──► MERGE (1 vs 3)  │
        │                             │
        ▼                             ▼
 Realtime Gold              Curated Silver (S3, hợp nhất)
   │                                  │
   ▼                                  ▼
Future ML Inference /          Curated Gold ──► S3 ──► BI/ML training
Alerting (low-latency)

============================================================
END
===
