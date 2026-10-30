# Giám sát & Kiểm thử chịu tải

## 1. Luồng metrics

| Nguồn | Endpoint | Job Prometheus | Nội dung chính |
|---|---|---|---|
| JMX exporter trong mỗi broker | `:7071/metrics` | `kafka-broker` | throughput, latency request, ISR, controller, JVM, CPU |
| kafka-exporter | `:9308/metrics` | `kafka-exporter` | consumer lag, offset, số broker |
| loadgen | `:8000/metrics` | `loadgen` | tốc độ gửi/nhận, ack latency, end-to-end latency |

Prometheus scrape mỗi **5 giây**, đủ mịn để thấy các đợt tăng tải. Dashboard Grafana **"KOL Platform - Kafka Cluster & Load"** được provision sẵn và đặt làm trang chủ.

## 2. Đọc dashboard

| Hàng | Panel | Bình thường | Đáng lo |
|---|---|---|---|
| Cluster health | Brokers up | 3 | < 3 |
| | Active controller | 1 | 0 (mất quorum) hoặc > 1 |
| | Under-replicated / Under min ISR / Offline | 0 | > 0 |
| | Total consumer lag | dao động quanh một mức thấp | tăng liên tục |
| Throughput | Messages in/s theo topic, theo broker | các broker xấp xỉ nhau | 1 broker gánh hết (leader lệch) |
| | Bytes in/out | out ≈ in × số consumer group | |
| Consumer lag | Lag theo group/topic, theo partition | gần 0, đều giữa các partition | lệch theo partition: key phân bố không đều (hot KOL) |
| Latency & saturation | Produce p99 | vài ms – vài chục ms | > 500 ms |
| | Request handler / network idle | > 50% | < 20%: broker bão hòa |
| Broker resources | CPU, heap, partition/leader count | heap dao động răng cưa (GC) | heap sát max |
| Load test | Throughput đạt vs target, latency p50/p95/p99, lỗi gửi | acked ≈ target, 0 lỗi | acked < target: đã chạm trần |

PromQL hữu ích (http://localhost:9090/graph):

```promql
# Tổng message/s vào cluster (không tính topic nội bộ)
sum(rate(kafka_server_brokertopicmetrics_messagesin_total{topic=~"kol\\..*"}[1m]))

# Lag theo consumer group
sum by (consumergroup) (kafka_consumergroup_lag)

# Thời gian ước tính để consumer xử lý hết lag (giây)
sum by (consumergroup, topic) (kafka_consumergroup_lag)
  / sum by (consumergroup, topic) (rate(kafka_consumergroup_current_offset[1m]))

# p99 produce latency của từng broker
kafka_network_requestmetrics_totaltimems{request="Produce", quantile="0.99"}
```

## 3. Alert

Định nghĩa trong [`monitoring/prometheus/alerts.yml`](../monitoring/prometheus/alerts.yml), dùng chung cho Compose và K8s. Xem tại http://localhost:9090/alerts.

| Alert | Điều kiện | Mức |
|---|---|---|
| KafkaBrokerDown | broker không scrape được 30s | critical |
| KafkaNoActiveController | số active controller ≠ 1 trong 1 phút | critical |
| KafkaOfflinePartitions | có partition không có leader | critical |
| KafkaUnderMinIsrPartitions | partition có ISR < min.insync.replicas | critical |
| KafkaUnderReplicatedPartitions | có partition thiếu replica trong 2 phút | warning |
| KafkaConsumerLagHigh | lag > 10 000 trong 2 phút | warning |
| KafkaConsumerLagGrowing | lag tăng > 50 msg/s liên tục 5 phút | warning |
| KafkaRequestHandlerSaturated | handler idle < 20% trong 5 phút | warning |
| KafkaProduceLatencyHigh | p99 produce > 500 ms trong 5 phút | warning |
| KafkaJvmHeapHigh | heap > 85% trong 5 phút | warning |

Có thể gắn Alertmanager (gửi Slack/Telegram/email) bằng cách thêm service `prom/alertmanager` và khối `alerting:` vào `prometheus.yml`.

## 4. Công cụ load test

### 4.1 loadgen (tải thực tế)

`loadtest/loadgen.py` sinh event JSON giống output của collector (khoảng 600–800 byte, key = `kol_id` trên 500 KOL). Producer dùng `acks=all` + idempotence + lz4. Script đo:

- **ack latency**: thời gian từ `produce()` tới khi đủ ISR xác nhận;
- **end-to-end latency** (consumer): `now - CreateTime` của record;
- throughput thực tế so với target.

```bash
# Compose
docker compose --profile loadtest up -d --build
docker compose logs -f loadgen-producer loadgen-consumer
LOADGEN_RATE=0 LOADGEN_DURATION=120 docker compose --profile loadtest up -d loadgen-producer   # max tốc độ
docker compose --profile loadtest up -d --scale loadgen-producer=4                           # 4 producer
docker compose --profile loadtest stop loadgen-producer loadgen-consumer

# Chạy trực tiếp trên host
pip install -r loadtest/requirements.txt
python loadtest/loadgen.py produce --rate 5000 --duration 60 --metrics-port 0
python loadtest/loadgen.py consume --group test-local --metrics-port 0
```

Khi kết thúc, producer in summary JSON và ghi vào `loadtest/results/producer-latest.json`:

```json
{ "mode": "produce", "target_rate": 2000, "duration_s": 300.0, "acked": 600000, "errors": 0,
  "throughput_msg_s": 2000.0, "ack_latency": { "p50_ms": 4.1, "p95_ms": 9.8, "p99_ms": 17.3, "max_ms": 120.5 } }
```
*(số liệu minh họa)*

### 4.2 Kafka perf tools (trần của cluster)

```bash
./scripts/perf-test.sh                                   # 500k x 1KB, acks=all, tốc độ tối đa
ACKS=1 ./scripts/perf-test.sh                            # so sánh acks=1 vs acks=all
RECORD_SIZE=100 RECORDS=2000000 ./scripts/perf-test.sh   # message nhỏ
```

## 5. Kịch bản kiểm thử đề xuất

| # | Kịch bản | Cách chạy | Kỳ vọng / cần ghi lại |
|---|---|---|---|
| T1 | Baseline | `LOADGEN_RATE=1000`, 5 phút | lag ≈ 0, p99 e2e thấp, 0 lỗi |
| T2 | Tăng tải từng bậc | 1k → 5k → 10k → 20k msg/s (mỗi bậc 3 phút, có thể `--scale`) | tìm bậc mà acked < target hoặc p99 tăng vọt: đó là **trần throughput** |
| T3 | Trần thô | `./scripts/perf-test.sh` với `acks=all` và `acks=1` | MB/s, records/s, latency; chi phí của durability |
| T4 | Burst (viral) | rate 1k ổn định, đột ngột `LOADGEN_RATE=0` trong 60s rồi trở lại | lag tăng rồi giảm về 0: Kafka **hấp thụ đột biến**, consumer xử lý dần |
| T5 | Consumer chậm | `LOADGEN_PROCESS_DELAY_MS=1` | lag tăng liên tục, alert `KafkaConsumerLagGrowing` |
| T6 | Scale consumer | T5 + `--scale loadgen-consumer=3` (K8s: HPA tự scale) | throughput consume tăng ~3x, lag giảm |
| T7 | Broker chết khi có tải | T1 + `./scripts/chaos-broker-failure.sh kafka-2` | 0 lỗi gửi (hoặc vài retry), leader chuyển, ISR phục hồi; ghi thời gian phục hồi |
| T8 | Mất quorum | `./scripts/chaos-broker-failure.sh kafka-2 kafka-3` | ghi bị từ chối (CP), không mất dữ liệu sau khi khôi phục |

### Mẫu bảng kết quả cho báo cáo

| Kịch bản | Target (msg/s) | Đạt được (msg/s) | MB/s | Ack p99 (ms) | E2E p99 (ms) | Lỗi | Max lag | Ghi chú |
|---|---|---|---|---|---|---|---|---|
| T1 | 1 000 | | | | | | | |
| T2 | 5 000 | | | | | | | |
| T2 | 10 000 | | | | | | | |
| T7 | 1 000 | | | | | | | thời gian ISR phục hồi: ... s |

Ghi kèm cấu hình máy (CPU, RAM cấp cho Docker) vì kết quả phụ thuộc nhiều vào phần cứng. Tất cả broker chạy trên cùng một máy nên con số chỉ mang tính **so sánh giữa các kịch bản**, không đại diện cho cluster thật.

## 6. Chaos test chi tiết

`./scripts/chaos-broker-failure.sh [broker...]` thực hiện:

1. In leader/ISR của `kol.loadtest`.
2. `docker stop` broker, chờ 15s.
3. In lại: leader đã chuyển sang broker khác, ISR còn 2.
4. Gửi 2 000 message `acks=all`: thành công nếu chỉ 1 broker chết.
5. Giữ broker chết tổng cộng `DOWN_SECONDS` (mặc định 30s), sau đó `docker start`.
6. Đo thời gian tới khi không còn under-replicated partition.
7. Chạy preferred leader election để cân bằng lại leader.

Nên mở Grafana song song để chụp màn hình cho báo cáo: `Brokers up` 3→2→3, `Under-replicated partitions` tăng rồi về 0, panel ISR shrink/expand và throughput loadgen không gián đoạn.
