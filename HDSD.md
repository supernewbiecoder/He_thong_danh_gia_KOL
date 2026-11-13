# Hướng dẫn sử dụng hệ thống đánh giá KOL

Hệ thống chạy trên **Kubernetes** (minikube, không cần Docker). Tài liệu này hướng dẫn cài đặt, triển khai, xem kết quả, vận hành và xử lý lỗi. Thiết kế chi tiết nằm trong [docs/kubernetes.md](docs/kubernetes.md) và [docs/architecture.md](docs/architecture.md); tình trạng các module xem ở [Da_lam_va_con_thieu.md](Da_lam_va_con_thieu.md).

## Mục lục
1. [Hệ thống gồm những gì](#1-hệ-thống-gồm-những-gì)
2. [Chuẩn bị](#2-chuẩn-bị)
3. [Triển khai](#3-triển-khai)
4. [Xem kết quả](#4-xem-kết-quả)
5. [Cấu hình thường chỉnh](#5-cấu-hình-thường-chỉnh)
6. [Dùng chung cả nhóm qua Tailscale](#6-dùng-chung-cả-nhóm-qua-tailscale)
7. [Cập nhật code](#7-cập-nhật-code)
8. [Kiểm thử](#8-kiểm-thử)
9. [Vận hành](#9-vận-hành)
10. [Dừng và dọn dẹp](#10-dừng-và-dọn-dẹp)
11. [Docker Compose (tùy chọn)](#11-docker-compose-tùy-chọn)
12. [Lỗi thường gặp](#12-lỗi-thường-gặp)

---

## 1. Hệ thống gồm những gì

```
Bluesky, YouTube ─► social-collectors ─► Kafka (3 broker) ─┬─► db-writer ─────────► PostgreSQL kol-db
   (Module 2)                             (Module 1)        │                       (1 primary + 2 replica,
                                                            └─► stream-processor ─┐  tự failover)
                                                                 (Module 3)       │        ▲
                                                                 kol.metrics.* ◄──┘        │
                                                                       └──► metrics-writer ┘
                                                                                     │
                                                          Grafana "KOL Leaderboard" ◄┘  (đọc replica)
```

| Thành phần trên Kubernetes | Loại | Vai trò |
|---|---|---|
| `kafka-0..2` | StatefulSet | Kafka KRaft, mỗi message 3 bản sao |
| `social-collectors` | Deployment | Module 2: firehose Bluesky, YouTube theo danh sách kênh |
| `stream-processor` | Deployment + HPA | Module 3: điểm KOL, từ khóa thịnh hành (2 pod, tự scale 1–6) |
| `kol-db-1..3` | Cluster CloudNativePG | Module 4: PostgreSQL, tự chuyển primary khi có sự cố |
| `db-writer` | Deployment | Lưu dữ liệu gốc vào `kol.social_events` |
| `metrics-writer` | Deployment | Lưu kết quả Module 3 vào `kol.kol_scores`, `kol.keyword_metrics`, `kol.kol_score_history` |
| `kol-db-backup` | CronJob | `pg_dump` lúc 2 giờ sáng mỗi ngày, giữ 14 ngày |
| `prometheus`, `grafana`, `kafka-ui`, `kafka-exporter` | Deployment | Giám sát và dashboard |

Toàn bộ nằm trong namespace `kol-system`.

---

## 2. Chuẩn bị

### Phần mềm
| Phần mềm | Ghi chú |
|---|---|
| [minikube](https://minikube.sigs.k8s.io/docs/start/) và [kubectl](https://kubernetes.io/docs/tasks/tools/) | Cluster Kubernetes chạy trên máy |
| Hyper-V (Windows 10/11 Pro) | Dùng làm máy ảo cho minikube, không cần Docker |
| Git Bash (Windows) | Các script `.sh` phải chạy bằng Git Bash |
| Python 3.11+ | Chỉ cần cho công cụ chạy trên máy: xem bảng xếp hạng, unit test, kiểm tra kết nối |

Máy cần trống khoảng 6 CPU, 10 GB RAM và 40 GB đĩa cho máy ảo minikube. Máy chỉ có 8 GB RAM thì dùng `--memory=6g`, khi đó hệ thống chạy chậm hơn.

### Bật Hyper-V và tạo cluster (làm một lần)
Mở **PowerShell bằng quyền Administrator**:
```powershell
Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V -All   # rồi khởi động lại máy
minikube start --driver=hyperv --container-runtime=containerd --cpus=6 --memory=10g --disk-size=40g
minikube addons enable metrics-server        # để HPA đọc được CPU
```

Driver Hyper-V cần quyền Administrator ở mỗi lần `minikube start`. macOS dùng `--driver=vfkit`, Linux dùng `--driver=kvm2`.

### Lấy code
```bash
git clone https://github.com/supernewbiecoder/He_thong_danh_gia_KOL.git
cd He_thong_danh_gia_KOL
```

### Môi trường Python (tùy chọn)
```bash
python -m venv .venv
source .venv/Scripts/activate        # Git Bash; PowerShell: .venv\Scripts\Activate.ps1; macOS/Linux: source .venv/bin/activate
pip install -r processor/requirements.txt -r collectors/requirements.txt
export PYTHONUTF8=1                  # Windows: để script in được tiếng Việt (PowerShell: $env:PYTHONUTF8=1)
```

---

## 3. Triển khai

Chạy trong Git Bash, từ thư mục gốc của repo:

```bash
./scripts/build-images.sh minikube   # build 5 image bên trong minikube, khoảng 1–3 phút
./scripts/k8s-deploy.sh              # lần đầu khoảng 5–10 phút (tải image PostgreSQL, Grafana...)
kubectl -n kol-system get pods
```

`k8s-deploy.sh` tự làm các bước sau:
1. Cài operator CloudNativePG.
2. Tạo mật khẩu ngẫu nhiên cho database. Secret đã có thì giữ nguyên.
3. Apply toàn bộ manifest.
4. Chờ Kafka, job tạo topic và PostgreSQL sẵn sàng.

Mọi pod phải ở trạng thái `Running`; riêng `kafka-topics-init`, `*-initdb`, `*-join` là `Completed`.

Thêm khóa YouTube (không bắt buộc; không có khóa thì chỉ thu thập Bluesky):
```bash
kubectl -n kol-system create secret generic collector-credentials --from-literal=youtube-api-key=KHOA_API
kubectl -n kol-system rollout restart deploy/social-collectors
```

---

## 4. Xem kết quả

### Mở cổng từ cluster ra máy
```bash
./scripts/k8s-expose.sh        # để cửa sổ này mở; Ctrl+C để dừng
```

| Dịch vụ | Địa chỉ |
|---|---|
| Grafana | http://127.0.0.1:3000 (`admin` / `admin`) |
| Kafka UI | http://127.0.0.1:8080 |
| Prometheus | http://127.0.0.1:9090 |
| Kafka | `127.0.0.1:30090,127.0.0.1:30091,127.0.0.1:30092` |
| PostgreSQL (primary) | `127.0.0.1:5432`, database `kol` |

### Grafana
- **KOL Leaderboard:**
  - Top KOL toàn thời gian và KOL đang nổi trong 15 phút.
  - Từ khóa và hashtag thịnh hành.
  - Biểu đồ điểm của top 5 KOL theo thời gian.
  - Số event lưu theo nền tảng.
- **KOL Platform - Kafka Cluster & Load:** sức khỏe Kafka, throughput, consumer lag, độ trễ, CPU và heap.

Các cột của bảng xếp hạng:

| Cột | Ý nghĩa |
|---|---|
| Điểm lũy kế (`engagement_score`) | Tổng điểm từ trước tới nay, được giữ qua các lần khởi động lại Module 3 |
| Điểm 15 phút (`window_score`) | Điểm từ các event thu thập trong 15 phút gần nhất |
| Điểm/phút | Điểm 15 phút chia 15 |

Công thức: điểm mỗi event = điểm theo loại event (post 5, follow 4, repost 3, comment và reply 2, like 1) + 1 × likes + 3 × comments + 5 × shares + 0,01 × views. Like, repost, reply và follow được tính cho **người nhận** (chủ bài viết, người được follow).

Cột KOL hiện mã tài khoản (ví dụ `bluesky:did:plc:...`) khi chưa biết handle, vì collector chỉ lấy profile khi KOL tự đăng bài.

### Bảng xếp hạng trên terminal
```bash
python processor/view_leaderboard.py --top 10      # cần k8s-expose.sh đang chạy
```

### Truy vấn PostgreSQL
```bash
PRIMARY=$(kubectl -n kol-system get pods -l cnpg.io/instanceRole=primary -o name)
kubectl -n kol-system exec "$PRIMARY" -c postgres -- psql -U postgres -d kol -c "
  SELECT coalesce(handle, kol_id) AS kol, engagement_score, window_score
  FROM kol.kol_scores ORDER BY engagement_score DESC LIMIT 10;"
```

| Bảng | Nội dung |
|---|---|
| `kol.social_events` | Mọi event gốc (34 cột) |
| `kol.kol_scores` | Điểm mới nhất của từng KOL |
| `kol.kol_score_history` | Điểm theo từng phút, giữ 7 ngày |
| `kol.keyword_metrics` | Từ khóa và hashtag, số lần trong 15 phút |

### Log và trạng thái
```bash
kubectl -n kol-system logs deploy/social-collectors --tail=20
kubectl -n kol-system logs deploy/stream-processor --tail=20     # tốc độ xử lý mỗi 5 giây
kubectl -n kol-system logs deploy/metrics-writer --tail=20
kubectl -n kol-system get cluster kol-db                          # trạng thái PostgreSQL, ai là primary
kubectl -n kol-system get hpa
```

---

## 5. Cấu hình thường chỉnh

Sau khi sửa, chạy lại `./scripts/k8s-deploy.sh`; các pod liên quan tự khởi động lại.

### Thu thập (Module 2): `k8s/base/kustomization.yaml`, mục `collectors-config`
| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `YOUTUBE_SEARCH_QUERIES` | 6 từ khóa | Tìm video để **phát hiện kênh mới** |
| `YOUTUBE_SEARCH_INTERVAL_SECONDS` | 86400 | Tìm kiếm tối đa một lần mỗi ngày (100 unit mỗi trang) |
| `YOUTUBE_CHANNEL_IDS` | trống | Kênh KOL muốn theo dõi cố định |
| `YOUTUBE_MAX_CHANNELS` | 25 | Số kênh theo dõi; kênh tìm được thêm vào cho tới giới hạn này |
| `YOUTUBE_RECENT_VIDEOS` | 5 | Số video mới nhất lấy của mỗi kênh mỗi chu kỳ |
| `COLLECTOR_POLL_SECONDS` | 1800 | Khoảng cách giữa hai chu kỳ YouTube |
| `BLUESKY_ENRICH_INTERACTIONS` | false | `true` sẽ gọi API cho mỗi lượt like để lấy nội dung bài gốc; tốc độ giảm từ khoảng 80 xuống khoảng 5 event/giây |

**Quota YouTube.** Hạn mức miễn phí là 10.000 unit mỗi ngày. Mỗi chu kỳ lấy video mới của 25 kênh qua playlist, chi tiết video và bình luận, tốn khoảng 150 unit. Với 48 chu kỳ mỗi ngày cộng một lần tìm kiếm (600 unit), tổng khoảng 8.000 unit. Log của collector in số unit đã dùng sau mỗi chu kỳ.

### Tính điểm (Module 3): `k8s/base/processor/deployment.yaml`
| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `WINDOW_MINUTES` | 15 | Độ dài cửa sổ "đang nổi" |
| `MAX_ACTIVE_KOLS` | 50000 | Số KOL giữ trong RAM mỗi pod; KOL lâu không hoạt động được lưu ở PostgreSQL rồi xóa khỏi RAM |
| `MIN_KEYWORD_KOLS` | 2 | Từ khóa phải do ít nhất từng ấy KOL nhắc tới mới được tính là thịnh hành |
| `WEIGHT_*` | xem công thức | Trọng số tính điểm, ví dụ `WEIGHT_SHARE_METRIC`; thêm vào phần `env` |

---

## 6. Dùng chung cả nhóm qua Tailscale

Máy chạy minikube làm máy chủ; các thành viên vào cùng một tailnet ([tailscale.com](https://tailscale.com), miễn phí cho tối đa 6 người).

**Cách đã chạy thử, dùng port-forward:**
```bash
KOL_EXTERNAL_HOST=<tên-máy>.<tailnet>.ts.net ./scripts/k8s-deploy.sh   # Kafka quảng bá tên Tailscale
TAILSCALE=1 ./scripts/k8s-expose.sh                                     # mở cổng và chia sẻ qua Tailscale
```

Thành viên dùng `<tên-máy>.<tailnet>.ts.net` thay cho `127.0.0.1` ở bảng mục 4, rồi kiểm tra kết nối:
```bash
python scripts/check_shared.py --kafka HOST:30090,HOST:30091,HOST:30092 \
  --postgres "postgresql://kol_readonly:MAT_KHAU@HOST:5432/kol"
```

Mật khẩu lấy bằng `kubectl -n kol-system get secret kol-db-readonly -o jsonpath='{.data.password}' | base64 -d`. Gửi mật khẩu qua kênh riêng, không commit.

**Cách K8s-native, dùng Tailscale operator (chưa chạy thử):** `./scripts/k8s-tailscale.sh`. Điều kiện cần ghi ở đầu script. Sau khi chạy, mỗi broker, PostgreSQL và Grafana có một tên riêng trong tailnet, và không cần mở cửa sổ port-forward.

---

## 7. Cập nhật code

Sau khi sửa code của một module, build lại image rồi khởi động lại Deployment tương ứng:

| Sửa thư mục | Lệnh |
|---|---|
| `collectors/` | `./scripts/build-images.sh minikube && kubectl -n kol-system rollout restart deploy/social-collectors` |
| `processor/` | `./scripts/build-images.sh minikube && kubectl -n kol-system rollout restart deploy/stream-processor` |
| `dbwriter/` | `./scripts/build-images.sh minikube && kubectl -n kol-system rollout restart deploy/db-writer deploy/metrics-writer` |
| `docker/kafka/` | `./scripts/build-images.sh minikube && kubectl -n kol-system rollout restart statefulset/kafka` |
| `k8s/`, `config/`, `monitoring/` | `./scripts/k8s-deploy.sh` |

`build-images.sh minikube` build cả 5 image, mất khoảng 1–3 phút. Kafka khởi động lại lần lượt từng broker nên không bị gián đoạn.

---

## 8. Kiểm thử

### Unit test (không cần cluster)
```bash
python -m unittest discover -s processor/tests -v      # Module 3: 10 test
python -m unittest discover -s collectors/tests -v     # Module 2: 8 test (gán điểm Bluesky, quota YouTube)
python processor/demo_local.py                         # mô phỏng Module 3 với 250 event
```

### Chịu lỗi trên Kubernetes
| Kịch bản | Lệnh | Kết quả đo ngày 13/11 |
|---|---|---|
| Broker chết khi có tải | `kubectl -n kol-system delete pod kafka-1` | 0 lỗi gửi; sẵn sàng lại sau 122 giây, đủ bản sao sau 179 giây |
| PostgreSQL primary chết | `kubectl -n kol-system delete pod <primary> --grace-period=0 --force` | Primary mới sau 40 giây; writer tự nối lại sau 51 giây |
| Module 3 khởi động lại | `kubectl -n kol-system rollout restart deploy/stream-processor` | 3.645 KOL được khôi phục điểm từ PostgreSQL |

Theo dõi trong lúc thử:
```bash
kubectl -n kol-system get pods -w
kubectl -n kol-system get cluster kol-db -w
```

### Tải và auto-scaling
```bash
kubectl -n kol-system run loadgen --image=kol-loadgen:1.0.0 --image-pull-policy=IfNotPresent --restart=Never -- \
  produce --bootstrap kafka:9092 --topic kol.loadtest --rate 1000 --duration 120 --metrics-port 0
kubectl -n kol-system logs -f loadgen          # cuối cùng in throughput, lỗi, độ trễ p50/p95/p99
kubectl -n kol-system delete pod loadgen

kubectl apply -k k8s/apps                      # bộ sinh tải chạy liên tục + HPA demo
kubectl -n kol-system get hpa -w
```

Trên minikube một node chạy cùng firehose thật, Kafka đạt khoảng 300 message/giây với `acks=all`. Con số này bị giới hạn bởi máy, không phải cụm nhiều máy thật.

---

## 9. Vận hành

### Sao lưu và khôi phục
```bash
kubectl -n kol-system create job --from=cronjob/kol-db-backup backup-now     # sao lưu ngay
kubectl -n kol-system logs job/backup-now                                    # in tên và dung lượng file
```

File nằm trong PVC `kol-db-backups`. Để lấy file ra máy, tạo một pod tạm (phần này chưa chạy thử):
```bash
kubectl -n kol-system run backup-shell --image=ghcr.io/cloudnative-pg/postgresql:16.6 --restart=Never --overrides='
{"spec":{"containers":[{"name":"backup-shell","image":"ghcr.io/cloudnative-pg/postgresql:16.6","command":["sleep","3600"],
"volumeMounts":[{"name":"b","mountPath":"/backups"}]}],"volumes":[{"name":"b","persistentVolumeClaim":{"claimName":"kol-db-backups"}}]}}'
kubectl -n kol-system exec backup-shell -- ls -lh /backups
kubectl cp kol-system/backup-shell:/backups/TEN_FILE.dump ./TEN_FILE.dump
kubectl -n kol-system delete pod backup-shell
```

Khôi phục vào database mới để kiểm tra: `pg_restore -h 127.0.0.1 -U postgres -d restore_test TEN_FILE.dump` khi `k8s-expose.sh` đang chạy. Tạo database `restore_test` trước, dùng mật khẩu trong secret `kol-db-superuser`.

### Sửa dữ liệu Bluesky cũ (một lần)
Dữ liệu lưu trước bản sửa gán điểm đang ghi like, repost, reply, follow cho người thực hiện. Sửa bằng:
```bash
PRIMARY=$(kubectl -n kol-system get pods -l cnpg.io/instanceRole=primary -o name)
kubectl -n kol-system exec -i "$PRIMARY" -c postgres -- psql -U postgres -d kol < dbwriter/migrations/001_fix_bluesky_kol_id.sql
```
Script chạy lại nhiều lần vẫn an toàn. Nó xóa điểm cũ để Module 3 tính lại từ dữ liệu mới.

### Đổi mật khẩu
1. Đổi mật khẩu trong database, chạy trên pod primary:
   ```bash
   kubectl -n kol-system exec "$PRIMARY" -c postgres -- psql -U postgres -c "ALTER ROLE kol_app PASSWORD 'MAT_KHAU_MOI'"
   ```
2. Ghi đúng mật khẩu đó vào Secret `kol-db-app`.
3. Khởi động lại các Deployment dùng nó: `db-writer`, `metrics-writer`, `stream-processor`.

Riêng `kol_readonly` do operator quản lý: chỉ cần sửa Secret `kol-db-readonly`, rồi khởi động lại `grafana`.

---

## 10. Dừng và dọn dẹp

| Muốn | Lệnh |
|---|---|
| Tạm dừng, giữ dữ liệu | `minikube stop` (lần sau `minikube start` với cùng tham số) |
| Xóa hệ thống, giữ cluster | `kubectl delete namespace kol-system` (**mất dữ liệu Kafka và PostgreSQL**) |
| Xóa hẳn cluster | `minikube delete` |

Firehose ghi liên tục vào PostgreSQL; ổ của mỗi instance có 2 GiB ở bản dev. Chạy lâu thì tăng `storage.size` trong `k8s/overlays/dev/postgres-resources.yaml`, hoặc tạm dừng collector bằng `kubectl -n kol-system scale deploy/social-collectors --replicas=0`.

---

## 11. Docker Compose (tùy chọn)

Docker Compose vẫn dùng được để thử nhanh trên một máy có Docker, nhưng **báo cáo và demo dùng Kubernetes**.

```bash
cp .env.example .env
docker compose --profile collectors --profile processor up -d --build   # không có PostgreSQL
NO_TAILSCALE=1 ./scripts/shared.sh up                                   # đầy đủ, có PostgreSQL và metrics-writer
```

Chi tiết: [docs/shared-infrastructure.md](docs/shared-infrastructure.md).

---

## 12. Lỗi thường gặp

| Triệu chứng | Nguyên nhân và cách xử lý |
|---|---|
| Pod `ErrImageNeverPull` hoặc `ImagePullBackOff` với image `kol-*` | Chưa build image trong minikube: chạy `./scripts/build-images.sh minikube` |
| `minikube start` báo lỗi quyền với Hyper-V | Mở PowerShell bằng quyền Administrator |
| `k8s-deploy.sh` chờ PostgreSQL rất lâu | Lần đầu phải tải image khoảng 150 MB. Xem `kubectl -n kol-system get pods -w` và `kubectl -n kol-system describe cluster kol-db` |
| `kubectl apply -k` báo lỗi file ngoài thư mục | Dùng `./scripts/k8s-deploy.sh`, không dùng `apply -k` cho `k8s/overlays/dev` |
| Không kết nối được `127.0.0.1:3000`, `:5432`, `:30090` | Chưa chạy hoặc đã tắt `./scripts/k8s-expose.sh` |
| Client Kafka trên máy kết nối được nhưng treo khi gửi | `KOL_EXTERNAL_HOST` lúc deploy khác địa chỉ client dùng; deploy lại với đúng giá trị |
| Bảng xếp hạng trống | Collector chưa có dữ liệu hoặc Module 3 chưa chạy: xem log `social-collectors`, `stream-processor`, `metrics-writer` |
| HPA hiện `<unknown>` | Chưa bật metrics-server: `minikube addons enable metrics-server` |
| YouTube báo `403 quotaExceeded` | Hết quota trong ngày: giảm `YOUTUBE_MAX_CHANNELS`, `YOUTUBE_RECENT_VIDEOS` hoặc tăng `COLLECTOR_POLL_SECONDS` |
| `UnicodeEncodeError` khi chạy script Python | `export PYTHONUTF8=1` |
| `bash\r: No such file or directory` | File `.sh` bị đổi sang CRLF: `git add --renormalize .` |
| PostgreSQL báo hết dung lượng | Tăng `storage.size` của Cluster `kol-db`, hoặc giảm lượng dữ liệu thu thập (mục 10) |

---

## Tài liệu liên quan
- [docs/kubernetes.md](docs/kubernetes.md): kiến trúc trên Kubernetes, chịu lỗi, scale
- [Da_lam_va_con_thieu.md](Da_lam_va_con_thieu.md): đã làm, còn thiếu
- [docs/integration-guide.md](docs/integration-guide.md): kết nối Kafka từ code, format message, cách gán `kol_id`
- [docs/module-3-stream-processor.md](docs/module-3-stream-processor.md): thiết kế Module 3
- [docs/monitoring-and-load-testing.md](docs/monitoring-and-load-testing.md): giám sát và kiểm thử chịu tải
