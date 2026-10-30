# Kiến trúc hạ tầng phân tán

Tài liệu mô tả thiết kế của Module 1 và lý do của từng quyết định, dùng khi báo cáo và trả lời hội đồng.

## 1. Vai trò của Kafka trong hệ thống

Collector (Module 2) không ghi thẳng vào DB mà publish vào Kafka. Kafka đóng vai trò:

- **Bộ đệm (buffer)**: khi có tải đột biến (một KOL viral, firehose Bluesky tăng vọt), message được giữ lại trên đĩa của broker và consumer xử lý theo tốc độ của mình. DB không bị nghẽn.
- **Tách rời (decoupling)**: collector và consumer có thể deploy, scale, restart độc lập. Khi consumer chết, dữ liệu vẫn còn trong Kafka (retention 3–7 ngày) và consumer đọc tiếp từ offset đã commit.
- **Song song hóa**: mỗi topic chia thành nhiều partition, và mỗi partition được giao cho đúng 1 consumer trong một consumer group, nên thêm consumer là tăng throughput (tối đa bằng số partition).
- **Phát lại (replay)**: khi sửa logic tính điểm, có thể reset offset để tính lại từ dữ liệu cũ.

## 2. Topology cluster

```
            ┌──────────── KRaft controller quorum (Raft, 3 voters) ────────────┐
            │                                                                  │
   ┌────────┴────────┐          ┌─────────────────┐          ┌────────────────┴┐
   │ kafka-1 (id 1)  │◄────────►│ kafka-2 (id 2)  │◄────────►│ kafka-3 (id 3)  │
   │ broker+controller│ replicate│ broker+controller│ replicate│ broker+controller│
   │ :19092 INTERNAL │          │ :19092 INTERNAL │          │ :19092 INTERNAL │
   │ :19093 CONTROLLER│          │ :19093 CONTROLLER│          │ :19093 CONTROLLER│
   │ :9092  EXTERNAL │          │ :9093  EXTERNAL │          │ :9094  EXTERNAL │
   │ :7071  metrics  │          │ :7071  metrics  │          │ :7071  metrics  │
   └─────────────────┘          └─────────────────┘          └─────────────────┘
```

### KRaft thay vì ZooKeeper
Từ Kafka 3.3, KRaft đã production-ready; Kafka 4.0 bỏ hẳn ZooKeeper. Metadata cluster được quản lý bởi quorum Raft nằm ngay trong Kafka, nên bớt được 3 container ZooKeeper, khởi động nhanh hơn và failover controller chỉ mất vài giây.

### Combined mode (broker + controller trên cùng node)
Production lớn thường tách controller riêng. Với quy mô đồ án và RAM laptop, chạy chung 3 node là đủ cho:
- quorum 3 voter → chịu được **1** controller chết (cần đa số 2/3);
- 3 replica dữ liệu.

Trên Kubernetes có thể tách role bằng cách tạo 2 StatefulSet (`KAFKA_PROCESS_ROLES=controller` / `broker`) mà không phải đổi gì ở client.

## 3. Độ bền dữ liệu & khả năng chịu lỗi

| Thiết lập | Giá trị | Ý nghĩa |
|---|---|---|
| `replication.factor` | 3 | Mỗi partition có 1 leader + 2 follower trên 3 broker khác nhau |
| `min.insync.replicas` | 2 | Với `acks=all`, chỉ xác nhận ghi khi ≥ 2 replica đã có message |
| Producer `acks=all` + `enable.idempotence=true` | khuyến nghị | Không mất message, không ghi trùng khi retry |
| `unclean.leader.election.enable` | false (mặc định) | Không bầu replica lạc hậu làm leader (tránh mất dữ liệu) |
| `auto.create.topics.enable` | false | Topic chỉ được tạo từ `config/topics.conf` (tránh gõ nhầm tên tạo topic rác) |

Kịch bản lỗi:

| Sự cố | Kết quả |
|---|---|
| 1 broker chết | Controller chuyển leader của các partition sang broker còn sống trong vài giây. ISR = 2 ≥ min ISR, nên **đọc/ghi vẫn bình thường, không mất dữ liệu**. Alert `KafkaBrokerDown`, `KafkaUnderReplicatedPartitions`. |
| Broker sống lại | Follower bắt kịp leader (ISR expand), sau đó preferred leader election cân bằng lại leader. |
| 2 broker chết | Quorum controller mất đa số và ISR = 1 < 2, nên ghi `acks=all` bị từ chối (`NOT_ENOUGH_REPLICAS`). Dữ liệu đã ghi **không mất**; hệ thống chọn tính nhất quán thay vì tính sẵn sàng (CP). |
| Consumer chết | Group rebalance, partition của nó được giao cho consumer khác và tiếp tục từ offset đã commit (at-least-once). |
| Container/pod bị kill | Docker `restart: unless-stopped` / Kubernetes StatefulSet tự tạo lại pod với **cùng node id và cùng volume**. |

Script `scripts/chaos-broker-failure.sh` minh họa các kịch bản này.

## 4. Thiết kế topic & partition

- **Tên topic** theo dạng `<domain>.<stage>.<source>`: `kol.raw.bluesky`, `kol.metrics.kol-scores`.
- **Một topic cho mỗi nguồn**: mỗi nguồn có tốc độ khác nhau (firehose >> API có quota), retention khác nhau, và consumer có thể chỉ đọc nguồn mình cần.
- **Số partition**: Bluesky 6 (tải cao nhất), YouTube/Reddit 3 (bị giới hạn bởi API quota). Số partition là **giới hạn trên của số consumer song song**, chỉ tăng được, không giảm được.
- **Message key = `kol_id`**: mọi event của một KOL vào cùng partition, nên consumer tính điểm cho KOL đó theo đúng thứ tự và có thể giữ state trong RAM mà không cần đồng bộ giữa các consumer.
- `kol.metrics.kol-scores` dùng **log compaction**: chỉ giữ giá trị mới nhất cho mỗi KOL, đóng vai trò "bảng điểm" có thể đọc lại từ đầu bất cứ lúc nào.
- `kol.dlq`: message không parse/xử lý được được đẩy sang đây thay vì chặn cả partition.

## 5. Listener & mạng

| Listener | Port (container) | Dùng cho |
|---|---|---|
| `INTERNAL` | 19092 (Compose) / 9092 (K8s) | Broker ↔ broker, các container/pod khác |
| `CONTROLLER` | 19093 / 9093 | Raft quorum giữa các controller |
| `EXTERNAL` | 9092–9094 (Compose) / NodePort 30090–30092 (K8s) | Ứng dụng chạy trực tiếp trên máy host |

Kafka trả cho client địa chỉ *advertised* của từng broker, nên mỗi môi trường mạng cần một listener riêng với địa chỉ phân giải được từ phía client. Đây là lý do có hai bộ địa chỉ bootstrap.

## 6. Giám sát

```
Kafka JVM ──(javaagent JMX exporter :7071)──┐
kafka-exporter :9308 (lag, offset) ─────────┼──► Prometheus (scrape 5s, alert rules) ──► Grafana
loadgen :8000 (latency, throughput) ────────┘
```

- **JMX exporter** gắn dạng javaagent qua `docker/kafka/entrypoint.sh`, không đặt `KAFKA_OPTS` ở mức container. Nếu đặt ở mức container, mọi lệnh CLI chạy bằng `docker exec` cũng nạp agent và lỗi trùng port 7071.
- **kafka-exporter** đọc offset của consumer group, nên tính được **consumer lag**. Đây là chỉ số quan trọng nhất để biết hệ thống có "chịu tải" được không: lag tăng liên tục nghĩa là consumer không theo kịp.
- Danh sách metrics và alert: [monitoring-and-load-testing.md](monitoring-and-load-testing.md).

## 7. Container & Kubernetes

| Khía cạnh | Docker Compose | Kubernetes |
|---|---|---|
| Danh tính broker | `container_name` + `KAFKA_NODE_ID` cố định | StatefulSet: `kafka-N` → node id `N`, DNS ổn định qua headless service |
| Dữ liệu | Named volume | PVC riêng mỗi pod (`volumeClaimTemplates`) |
| Tự phục hồi | `restart: unless-stopped` + healthcheck | Controller tạo lại pod, gắn lại đúng PVC; probe startup/readiness/liveness |
| Bảo vệ khi bảo trì | – | PodDisruptionBudget `maxUnavailable: 1` |
| Scale ứng dụng | `--scale service=N` | HPA (CPU) hoặc KEDA (consumer lag) |
| Khám phá metrics | Static targets | `kubernetes_sd_configs` theo annotation `prometheus.io/*` |

Không dùng Strimzi operator để giữ manifest tối giản, dễ đọc và dễ giải thích (mọi thứ là StatefulSet/Service thuần). Strimzi là lựa chọn hợp lý nếu triển khai thật lâu dài.
