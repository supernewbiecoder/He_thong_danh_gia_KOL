# Hướng dẫn tích hợp cho các module

Dành cho Module 2 (collector), Module 3 (stream consumer), Module 4 (DB) và Module 5 (API): cách kết nối vào Kafka cluster của Module 1.

## 1. Địa chỉ kết nối

| Code của bạn chạy ở đâu | `bootstrap.servers` |
|---|---|
| Trực tiếp trên máy (python/node chạy local) | `localhost:9092,localhost:9093,localhost:9094` |
| Container trong Docker Compose (network `kol-net`) | `kafka-1:19092,kafka-2:19092,kafka-3:19092` |
| Pod trong Kubernetes (namespace `kol-system`) | `kafka:9092` (cùng namespace) hoặc `kafka.kol-system.svc.cluster.local:9092` |
| Máy host → Kubernetes local (Docker Desktop) | `localhost:30090,localhost:30091,localhost:30092` |

Nên đọc địa chỉ từ biến môi trường `BOOTSTRAP_SERVERS` thay vì hard-code.

### Cho container của module khác vào cùng network (Compose)

Trong `docker-compose.yml` của module bạn:

```yaml
services:
  bluesky-collector:
    build: .
    environment:
      BOOTSTRAP_SERVERS: kafka-1:19092,kafka-2:19092,kafka-3:19092
networks:
  default:
    name: kol-net
    external: true
```

Chạy hạ tầng trước (`docker compose up -d` ở thư mục gốc), sau đó mới chạy compose của module.

## 2. Topic

| Topic | Ai ghi | Ai đọc | Key |
|---|---|---|---|
| `kol.raw.bluesky` / `kol.raw.youtube` / `kol.raw.reddit` | Module 2 | Module 3 | `kol_id` |
| `kol.metrics.kol-scores` | Module 3 | Module 4, 5 | `kol_id` (compacted: chỉ giữ bản mới nhất) |
| `kol.metrics.keywords` | Module 3 | Module 4, 5 | keyword/hashtag |
| `kol.dlq` | tất cả | debug | tùy |

Cần topic mới: thêm một dòng vào [`config/topics.conf`](../config/topics.conf) rồi báo Module 1 (hoặc tự chạy `docker compose up kafka-init`). Cluster **không tự tạo topic**.

## 3. Format message

JSON UTF-8, theo [`config/schemas/social-event.schema.json`](../config/schemas/social-event.schema.json) (đề xuất, Module 2 và 3 thống nhất lại nếu cần):

```json
{
  "event_id": "5f0c3c1e-8a36-4d52-9b7a-2b1f4c1e9a10",
  "platform": "youtube",
  "event_type": "comment",
  "content_id": "UgxK3...",
  "parent_id": "dQw4w9WgXcQ",
  "kol_id": "youtube:UC_x5XG1OV2P6uZZ5FSM9Ttw",
  "author": { "id": "youtube:UCabc", "handle": "@viewer", "followers": null },
  "text": "Review rất có tâm #skincare",
  "hashtags": ["skincare"],
  "mentions": [],
  "metrics": { "likes": 12, "shares": 0, "comments": 1, "views": 0 },
  "lang": "vi",
  "created_at": "2026-10-30T08:00:00.000Z",
  "ingested_at": "2026-10-30T08:00:01.250Z",
  "source": { "collector": "youtube-worker", "version": "1.0" }
}
```

**Key của message = `kol_id`** (bytes UTF-8). Nhờ vậy mọi event của cùng một KOL vào cùng partition và giữ thứ tự, nên consumer có thể giữ state của KOL đó trong RAM.

## 4. Producer (Module 2, Module 3 khi ghi kết quả)

Python với [`confluent-kafka`](https://pypi.org/project/confluent-kafka/) (`pip install confluent-kafka`):

```python
import json, os
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"),
    "client.id": "bluesky-collector",
    "acks": "all",                 # bắt buộc: chờ đủ min.insync.replicas (=2)
    "enable.idempotence": True,    # retry không tạo bản ghi trùng
    "compression.type": "lz4",
    "linger.ms": 10,               # gom batch, tăng throughput
    "batch.size": 131072,
})

def on_delivery(err, msg):
    if err:
        print(f"delivery failed: {err}")   # log / đếm lỗi / đẩy vào kol.dlq

def publish(event: dict):
    producer.produce("kol.raw.bluesky", key=event["kol_id"].encode(),
                     value=json.dumps(event).encode(), on_delivery=on_delivery)
    producer.poll(0)               # gọi thường xuyên để xử lý callback

# khi tắt chương trình (SIGTERM):
producer.flush(10)
```

Chú ý:
- **Đa luồng**: một `Producer` thread-safe, dùng chung cho mọi thread worker. Không tạo producer cho mỗi message.
- Gặp `BufferError` (hàng đợi local đầy) nghĩa là đang gửi nhanh hơn Kafka nhận: gọi `producer.poll(0.1)` rồi thử lại.
- Không gọi `flush()` sau mỗi message vì làm throughput giảm hàng trăm lần.

## 5. Consumer (Module 3, Module 4)

```python
import json, os, signal
from confluent_kafka import Consumer

running = True
signal.signal(signal.SIGTERM, lambda *_: globals().update(running=False))

consumer = Consumer({
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"),
    "group.id": "stream-processor",       # mọi instance cùng group sẽ chia nhau partition
    "auto.offset.reset": "earliest",      # group mới: đọc từ đầu
    "enable.auto.commit": False,          # tự commit sau khi xử lý xong (at-least-once)
})
consumer.subscribe(["kol.raw.bluesky", "kol.raw.youtube", "kol.raw.reddit"])

try:
    while running:
        msgs = consumer.consume(num_messages=500, timeout=1.0)
        for msg in msgs:
            if msg.error():
                continue
            try:
                event = json.loads(msg.value())
                # ... cập nhật state trong RAM ...
            except Exception:
                pass  # gửi raw message vào kol.dlq kèm lý do lỗi
        if msgs:
            consumer.commit(asynchronous=True)
finally:
    consumer.close()                      # commit lần cuối + rời group sạch sẽ (rebalance nhanh)
```

Chú ý:
- **Số instance trong một group ≤ số partition** của topic. Instance dư sẽ ngồi chơi.
- Đặt `group.id` riêng cho mỗi module (`stream-processor`, `db-writer`...). Hai group khác nhau đều nhận **toàn bộ** message.
- Giữ thời gian xử lý mỗi batch < `max.poll.interval.ms` (mặc định 5 phút), nếu không consumer bị đá khỏi group.
- At-least-once nên có thể nhận trùng message sau rebalance. Dùng `event_id` để loại trùng nếu cần.
- Lag của group hiện trên Grafana (hàng "Consumer lag") và Kafka UI. Chính Module 3 nên theo dõi chỉ số này khi demo.

Node.js (Module 5) có thể dùng [`kafkajs`](https://kafka.js.org/) với cùng địa chỉ và cùng nguyên tắc.

## 6. Đưa service của bạn lên Kubernetes

Dùng mẫu [`k8s/apps/stream-consumer-template.yaml`](../k8s/apps/stream-consumer-template.yaml):

1. Build image: `docker build -t my-service:1.0.0 .` (kind/minikube: load image như trong `scripts/build-images.sh`).
2. Sửa `name`, `image`, env, `maxReplicas` (≤ số partition).
3. `kubectl apply -n kol-system -f my-service.yaml`.
4. Muốn Prometheus thu metrics của service: expose `/metrics` và đặt annotation `prometheus.io/scrape: "true"`.

## 7. Kiểm tra nhanh bằng CLI

```bash
# Đọc 5 message cuối từ host (cần Git Bash trên Windows)
MSYS_NO_PATHCONV=1 docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:19092 --topic kol.raw.bluesky --from-beginning --max-messages 5

# Gửi thử 1 message có key
MSYS_NO_PATHCONV=1 docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:19092 --topic kol.raw.bluesky \
  --property parse.key=true --property key.separator='|' <<< 'bluesky:kol-1|{"hello":"world"}'

# Lag của consumer group
./scripts/cluster-status.sh groups
```
