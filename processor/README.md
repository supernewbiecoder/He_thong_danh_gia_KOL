# Module 3 – Xử lý Luồng Thời gian thực (Stream Processor)

Module 3 tiêu thụ các sự kiện mạng xã hội từ Kafka (`kol.raw.bluesky`, `kol.raw.youtube`), tính toán **điểm tương tác KOL** và **tần suất từ khóa / hashtag thịnh hành** trong bộ nhớ (in-memory sliding window), sau đó xuất kết quả sang `kol.metrics.kol-scores` và `kol.metrics.keywords` cho Module 4 (DB) và Module 5 (API / Dashboard).

## 1. Kiến trúc & Cơ chế hoạt động

1. **At-Least-Once + Idempotent Deduplication:**
   - Consumer group `stream-processor` tắt `enable.auto.commit`, chỉ commit offset sau khi đã đẩy kết quả sang Kafka.
   - Bộ đệm LRU (`DedupCache`) giữ 200.000 `event_id` gần nhất để loại bỏ các bản ghi bị đọc lặp lại khi consumer rebalance.
2. **Xử lý lỗi (Dead-Letter Queue – `kol.dlq`):**
   - Mọi message sai cú pháp JSON hoặc thiếu trường bắt buộc theo [`config/schemas/social-event.schema.json`](../config/schemas/social-event.schema.json) đều được bọc kèm nguyên nhân lỗi và đẩy vào `kol.dlq`, không làm nghẽn luồng xử lý.
3. **Công thức tính điểm KOL (`KOLRecord`):**
   - Mỗi sự kiện đóng góp:
     $$\text{EventScore} = w_{\text{event}} + 1.0 \cdot \text{likes} + 3.0 \cdot \text{comments} + 5.0 \cdot \text{shares} + 0.01 \cdot \text{views}$$
     trong đó $w_{\text{post}}=5, w_{\text{repost}}=3, w_{\text{comment}}=2, w_{\text{reply}}=2, w_{\text{like}}=1$.
   - Lưu song song **Tổng lũy kế (`engagement_score`, `metrics_total`)** và **Cửa sổ trượt 15 phút (`window_score`, `metrics_window`, `velocity_per_min`)**.
4. **Thống kê Keyword & Hashtag (`KeywordRecord`):**
   - Tách hashtag từ mảng `hashtags` + `#tag` trong `text`, đồng thời tách từ khóa có nghĩa từ `text` (loại bỏ stopwords tiếng Việt & tiếng Anh, URL, `@mention`).
   - Đếm số lần xuất hiện theo cửa sổ trượt (`count`), phân rã theo từng nền tảng (`bluesky`, `youtube`, `reddit`) và đếm số lượng KOL duy nhất (`unique_kols`) nhắc tới từ khóa đó.
5. **Rebalance-safe & Graceful Shutdown:**
   - Khi thu hồi partition (`on_revoke`) hoặc nhận tín hiệu `SIGINT`/`SIGTERM`, processor tự động flush toàn bộ state đang dở dang và commit offset đồng bộ trước khi đóng.

## 2. Chạy thử độc lập (Không cần chờ Module 2)

Bạn có thể dùng ngay bộ sinh tải `loadtest/loadgen.py` có sẵn trong dự án để giả lập dữ liệu từ Module 2 bắn vào `kol.raw.bluesky`:

### Cách 1: Chạy bằng Docker Compose
```bash
# 1. Khởi động cụm Kafka + Monitoring
docker compose up -d --build

# 2. Khởi động Module 3 (stream-processor)
docker compose --profile processor up -d --build

# 3. Giả lập Module 2 bắn dữ liệu vào kol.raw.bluesky
docker compose run --rm -e LOADGEN_TOPIC=kol.raw.bluesky -e LOADGEN_RATE=200 -e LOADGEN_DURATION=60 loadgen-producer

# 4. Xem log xử lý của Module 3
docker compose logs -f stream-processor
```

### Cách 2: Chạy trực tiếp bằng Python trên máy (kết nối local hoặc Tailscale của nhóm)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r processor/requirements.txt

# Chạy Unit Test kiểm tra logic
python -m unittest discover -s processor/tests -v

# Chạy Stream Processor (mặc định nối vào 127.0.0.1:9092..9094, hoặc truyền --bootstrap HOST:9092...)
python -m processor.processor --bootstrap 127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094
```

Ở một cửa sổ Terminal khác, bắn dữ liệu giả lập vào `kol.raw.bluesky`:
```bash
source .venv/bin/activate
python loadtest/loadgen.py produce --bootstrap 127.0.0.1:9092 --topic kol.raw.bluesky --rate 100 --duration 30 --metrics-port 0
```

## 3. Kiểm tra kết quả đầu ra

Mở **Kafka UI** tại `http://localhost:8080` hoặc đọc bằng lệnh CLI:
```bash
# Xem điểm KOL vừa tính được
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:19092 --topic kol.metrics.kol-scores \
  --from-beginning --max-messages 5 --property print.key=true

# Xem top từ khóa / hashtag thịnh hành
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:19092 --topic kol.metrics.keywords \
  --from-beginning --max-messages 5 --property print.key=true
```
