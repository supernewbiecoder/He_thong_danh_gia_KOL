# Đã làm và còn thiếu

*Cập nhật ngày 13/11/2026: chuyển sang Kubernetes, sửa các lỗi về điểm KOL, thêm replication cho PostgreSQL.*

Tóm gọn: **project là hệ thống chấm điểm KOL theo thời gian thực, chạy trên Kubernetes.** Nó cào dữ liệu Bluesky và YouTube, cho qua Kafka, tính điểm KOL và từ khóa thịnh hành ngay trên luồng, rồi lưu cả dữ liệu gốc lẫn kết quả vào PostgreSQL có replication.

Đã chạy thật trên minikube với dữ liệu Bluesky thật: **cào → Kafka → tính điểm → PostgreSQL → bảng xếp hạng trên Grafana**. Còn thiếu Module 5 (API và giao diện web riêng).

## Luồng dữ liệu hiện tại

```
 Bluesky (firehose)  ─┐
 YouTube Data API    ─┴─► [collectors] ──► KAFKA ──┬─► [db-writer] ──────────► POSTGRESQL (CloudNativePG)
                            ✅ Module 2     ✅ M1    │                           1 primary + 2 replica ✅
                                                   └─► [stream-processor] ─┐         ▲
                                                         ✅ Module 3       │         │
                                                         kol.metrics.* ────┴─► [metrics-writer] ✅
                                                                                     │
                                         Grafana "KOL Leaderboard" ✅ ◄───────────────┘
                                         API + giao diện web (Module 5) ❌
```

## Lịch sử commit

| Ngày | Người | Nội dung |
|---|---|---|
| 30/10 | NguyenDucManh | Hạ tầng: Kafka 3 node, Docker, Kubernetes, giám sát, load test |
| 05/11 | NguyenDucManh | Hạ tầng dùng chung: PostgreSQL, Tailscale, sao lưu |
| 12/11 | manh-git | Module 2: cào YouTube và Bluesky |
| 12/11 | manh-git | Module 4 bản tối thiểu: ghi Kafka vào PostgreSQL |
| 13/11 | trangnguyen2k5-sketch | Module 3: tính điểm KOL và từ khóa thịnh hành (PR #1) |
| 13/11 | *(chưa commit)* | Chuyển sang Kubernetes, sửa điểm KOL, CloudNativePG, metrics-writer, dashboard bảng xếp hạng |

## Từng phần đã làm

### Module 1: Kafka và Kubernetes
- **Kafka:** 3 broker KRaft, mỗi message 3 bản sao, cần 2 bản xác nhận; 7 topic giới hạn cả thời gian lẫn dung lượng.
- **Kubernetes là môi trường chính.**
  - Chạy trên minikube, không cần Docker: image được build ngay trong minikube (`build-images.sh minikube`).
  - `k8s-deploy.sh` triển khai toàn bộ chỉ bằng một lệnh.
  - `k8s-expose.sh` mở cổng ra máy và tự nối lại khi pod khởi động lại.
  - `k8s-tailscale.sh` để chia sẻ cho cả nhóm (chưa chạy thử).
- **Giám sát:** Prometheus (10 cảnh báo), Grafana với 2 dashboard (hạ tầng và bảng xếp hạng KOL), Kafka UI.
- **Lỗi hạ tầng đã sửa khi chạy thật:**
  - JMX exporter thu quá nhiều metrics, khiến mỗi broker ăn trọn 1 CPU. Đã thu hẹp lại: load của node giảm từ 25 xuống 3, mỗi lần lấy metrics từ hơn 5 giây xuống khoảng 0,1 giây.
  - Script Kubernetes không chạy được trong Git Bash trên Windows do đường dẫn kiểu `/c/...`.

### Module 2: thu thập ([collectors/main.py](collectors/main.py))
- **Bluesky:**
  - Đọc firehose toàn mạng: post, reply, like, repost, follow, xóa bài.
  - **Đã sửa cách gán điểm:** like, repost, reply được tính cho chủ bài viết, follow cho người được follow. Trước đây chúng bị tính cho người thực hiện.
  - **Đã sửa chỗ nghẽn tốc độ:** trước đây collector gọi API cho từng lượt like và tra profile của mọi người đi like. Tốc độ tăng từ khoảng 5 lên khoảng 80 event/giây.
- **YouTube:**
  - **Đã chuyển sang theo dõi danh sách kênh** qua playlist uploads (1 unit mỗi kênh) thay vì tìm kiếm mỗi chu kỳ (100 unit mỗi lần).
  - Tìm kiếm chỉ chạy một lần mỗi ngày để phát hiện kênh mới.
  - Ước tính khoảng 8.000 trên 10.000 unit mỗi ngày.
- Có 8 unit test.

### Module 3: tính điểm ([processor/](processor/))
- **Cách tính:**
  - Điểm mỗi event = điểm theo loại event + 1 × likes + 3 × comments + 5 × shares + 0,01 × views.
  - Mỗi KOL có điểm lũy kế và điểm trong 15 phút gần nhất.
  - Từ khóa thịnh hành được đếm theo cửa sổ 15 phút.
  - Kết quả được đẩy ra `kol.metrics.*` mỗi 5 giây.
- **Đã sửa và bổ sung:**
  - Handle của KOL chỉ lấy từ event của chính KOL.
  - Cửa sổ 15 phút tính theo thời điểm event được thu thập, nên event cũ đọc lại không làm sai "đang nổi".
  - Giới hạn số KOL giữ trong RAM (50.000 mỗi pod), tránh pod bị kill vì hết bộ nhớ.
  - **Khôi phục điểm lũy kế từ PostgreSQL** sau khi khởi động lại hoặc rebalance.
  - Từ khóa tiếng Việt được tách thành cụm hai âm tiết ("sản phẩm", "chi tiết").
  - Từ khóa phải do ít nhất 2 KOL nhắc tới mới tính là thịnh hành.
- Chạy 2 pod, HPA tự scale từ 1 đến 6 pod. Có 10 unit test.

### Module 4: lưu trữ
- **PostgreSQL chạy bằng CloudNativePG:** 1 primary và 2 replica, tự failover; Service `kol-db-rw` cho ghi, `kol-db-ro` cho đọc.
- **`db-writer`:** lưu dữ liệu gốc vào `kol.social_events`, upsert theo `event_id`, chỉ commit offset sau khi PostgreSQL đã lưu.
- **`metrics-writer` (mới):** lưu kết quả Module 3 vào `kol.kol_scores`, `kol.keyword_metrics` và `kol.kol_score_history` (điểm theo phút, giữ 7 ngày).
- **Sao lưu:** CronJob `pg_dump` mỗi ngày, giữ 14 ngày; đã chạy thử, ra file 18 MB.
- **Script sửa dữ liệu Bluesky cũ:** [dbwriter/migrations/001_fix_bluesky_kol_id.sql](dbwriter/migrations/001_fix_bluesky_kol_id.sql).

## Đã kiểm chứng

| Nội dung | Kết quả | Ngày |
|---|---|---|
| Triển khai toàn bộ lên minikube (containerd, không Docker) | Mọi pod chạy; image build trong minikube mất 72 giây | 13/11 |
| Pipeline với dữ liệu Bluesky thật | Khoảng 50.000 event lưu; 14.564/14.585 like gán đúng chủ bài; khoảng 11.000 KOL có điểm | 13/11 |
| Grafana "KOL Leaderboard" | Datasource kết nối qua replica (TLS); cả 9 truy vấn trả dữ liệu thật | 13/11 |
| Truy cập từ máy qua `k8s-expose.sh` | Kafka đọc/ghi được, PostgreSQL đúng quyền, `view_leaderboard.py` chạy | 13/11 |
| Xóa pod `kafka-1` khi đang có tải | 35.231 message, **0 lỗi**; broker sẵn sàng lại sau 122 giây, đủ bản sao sau 179 giây | 13/11 |
| Kill đột ngột pod primary PostgreSQL | Primary mới sau 40 giây; writer tự nối lại sau 51 giây, không mất dữ liệu | 13/11 |
| Khởi động lại Module 3 | 3.645 KOL được khôi phục điểm từ PostgreSQL | 13/11 |
| HPA Module 3 | Đọc được CPU (32% trên ngưỡng 70%) | 13/11 |
| Unit test Module 2 / Module 3 | 8/8 và 10/10 qua | 13/11 |
| Docker Compose: tắt broker khi có tải (bản cũ) | 149.999 message, 0 lỗi | 05/11 |

Trên minikube một node chạy cùng firehose thật, Kafka đạt khoảng 300 message/giây với `acks=all`. Đây là giới hạn của máy thử, không phải của thiết kế.

## Chưa có

| Phần | Thiếu gì |
|---|---|
| **Module 5, API và giao diện web** | Chưa có REST API và giao diện riêng. Tạm thời dùng dashboard Grafana "KOL Leaderboard". API nên đọc PostgreSQL qua `kol-db-ro` bằng tài khoản `kol_readonly`. |
| **Handle của KOL** | KOL chỉ được nhận like mà không tự đăng bài trong lúc collector chạy thì chưa có tên hiển thị. Gợi ý: API tra profile (Bluesky `getProfiles`) cho top KOL khi hiển thị. |
| **Chia sẻ qua Tailscale operator** | Script đã có (`k8s-tailscale.sh`) nhưng chưa chạy thử. Cách port-forward kết hợp Tailscale đã chạy được. |
| **Cụm nhiều máy** | Mọi thứ chạy trên một node minikube: chịu được lỗi từng pod, chưa chịu được lỗi cả máy. |
| **Reddit** | Đã bỏ vì API bị từ chối. Đề xuất gửi hội đồng ghi 3 nguồn nên cần cập nhật. |

## Muốn đọc code thì bắt đầu từ đâu
1. [HDSD.md](HDSD.md): cài đặt và chạy trên Kubernetes.
2. [docs/kubernetes.md](docs/kubernetes.md): kiến trúc trên Kubernetes, chịu lỗi, scale.
3. Đọc theo chiều dữ liệu chảy: [collectors/main.py](collectors/main.py), rồi [processor/state.py](processor/state.py), rồi [dbwriter/main.py](dbwriter/main.py) và [dbwriter/metrics_writer.py](dbwriter/metrics_writer.py).
4. [docs/module-3-stream-processor.md](docs/module-3-stream-processor.md): định dạng đầu ra Module 3. Người làm Module 5 cần đọc file này.
