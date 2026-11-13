# Triển khai trên Kubernetes

Kubernetes là môi trường chạy chính của hệ thống. Docker Compose chỉ còn dùng để thử nhanh trên một máy.

## 1. Thành phần

```
namespace kol-system
├── StatefulSet kafka (kafka-0..2, KRaft) + PVC + PDB      Module 1
├── Job kafka-topics-init (tạo topic từ config/topics.conf)
├── Deployment social-collectors + PVC cursor               Module 2
├── Deployment stream-processor (2 pod) + HPA 1–6           Module 3
├── Cluster kol-db (CloudNativePG: 1 primary + 2 replica)   Module 4
│     Service kol-db-rw (primary), kol-db-ro (replica)
├── Deployment db-writer, metrics-writer                    Module 4
├── CronJob kol-db-backup (pg_dump mỗi ngày, giữ 14 ngày)
├── Deployment prometheus, grafana, kafka-ui, kafka-exporter
└── k8s/apps: loadgen-producer, loadgen-consumer + HPA (load test)
```

| Thư mục / file | Nội dung |
|---|---|
| `k8s/base/kafka/` | Broker KRaft: node id = số thứ tự pod; NodePort `30090–30092` cho client ngoài cluster |
| `k8s/base/postgres/cluster.yaml` | PostgreSQL 16 do operator CloudNativePG quản lý: replication, tự failover |
| `k8s/base/postgres/backup.yaml` | CronJob sao lưu `pg_dump` vào PVC `kol-db-backups` |
| `k8s/base/collectors/` | Module 2; khóa YouTube nằm trong Secret tùy chọn `collector-credentials` |
| `k8s/base/processor/` | Module 3 + HPA |
| `k8s/base/dbwriter/` | `db-writer` (dữ liệu gốc) và `metrics-writer` (kết quả Module 3) |
| `k8s/base/monitoring/` | Prometheus, Grafana (dashboard Kafka + KOL Leaderboard), kafka-exporter |
| `k8s/overlays/dev/` | Giảm tài nguyên cho cluster trên laptop |
| `k8s/apps/` | Load test và demo auto-scaling |
| `k8s/addons/keda/` | Scale theo consumer lag (tùy chọn) |

## 2. Chuẩn bị cluster

Khuyến nghị dùng **minikube**: chạy được không cần Docker, và image được build ngay bên trong minikube.

| Hệ điều hành | Lệnh |
|---|---|
| Windows 10/11 Pro (Hyper-V) | Mở PowerShell **quyền Administrator**: `Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V -All` (khởi động lại máy), rồi `minikube start --driver=hyperv --container-runtime=containerd --cpus=6 --memory=10g --disk-size=40g` |
| macOS | `minikube start --driver=vfkit --container-runtime=containerd --cpus=6 --memory=10g` |
| Linux | `minikube start --driver=kvm2 --container-runtime=containerd --cpus=6 --memory=10g` |

Cài minikube và kubectl theo https://minikube.sigs.k8s.io/docs/start/. Cả hệ thống cần khoảng 6 CPU và 8–10 GB RAM.

Nếu được phép dùng Docker, có thể dùng Kubernetes có sẵn trong Docker Desktop (*Settings → Kubernetes → Enable*). Khi đó build image bằng `./scripts/build-images.sh` (không có tham số).

## 3. Triển khai

```bash
./scripts/build-images.sh minikube     # build 5 image bên trong minikube, khoảng 1–3 phút
./scripts/k8s-deploy.sh                # cài CloudNativePG, tạo secret, apply, chờ Kafka và PostgreSQL
kubectl -n kol-system get pods
```

`k8s-deploy.sh` làm những việc sau:
1. Cài operator CloudNativePG nếu cluster chưa có.
2. Tạo 3 Secret mật khẩu ngẫu nhiên `kol-db-app`, `kol-db-readonly`, `kol-db-superuser`. Secret đã có thì không bao giờ bị ghi đè.
3. Render `k8s/overlays/dev` và apply.
4. Chờ Kafka, job tạo topic và cụm PostgreSQL sẵn sàng.

Base dùng file nằm ngoài thư mục `k8s/` (topic, alert, dashboard), nên **không** dùng được `kubectl apply -k k8s/overlays/dev`. Xem trước manifest bằng `./scripts/k8s-deploy.sh dev --dry`.

Khóa YouTube (tùy chọn):
```bash
kubectl -n kol-system create secret generic collector-credentials --from-literal=youtube-api-key=KEY
kubectl -n kol-system rollout restart deploy/social-collectors
```

Xem mật khẩu database:
```bash
kubectl -n kol-system get secret kol-db-app -o jsonpath='{.data.password}' | base64 -d
```

Mọi lệnh `kubectl ... | base64 -d` chạy trong Git Bash.

## 4. Truy cập từ máy

```bash
./scripts/k8s-expose.sh      # để cửa sổ này mở; Ctrl+C để dừng
```

| Dịch vụ | Địa chỉ |
|---|---|
| Kafka | `127.0.0.1:30090,127.0.0.1:30091,127.0.0.1:30092` |
| PostgreSQL (primary) | `127.0.0.1:5432`, database `kol` |
| Grafana | http://127.0.0.1:3000 (`admin` / `admin`) |
| Kafka UI | http://127.0.0.1:8080 |
| Prometheus | http://127.0.0.1:9090 |

Script dùng `kubectl port-forward` và tự nối lại khi pod khởi động lại. Riêng Kafka, mỗi broker được forward một cổng riêng, khớp với địa chỉ broker tự quảng bá (`KOL_EXTERNAL_HOST:30090+N`), nên client ngoài cluster kết nối được.

Với Docker Desktop không cần script này cho Kafka, vì NodePort đã mở sẵn trên `127.0.0.1`.

### Cho cả nhóm cùng dùng (Tailscale)

**Cách đã chạy thử, dùng port-forward:**
```bash
KOL_EXTERNAL_HOST=<tên-máy>.<tailnet>.ts.net ./scripts/k8s-deploy.sh   # broker quảng bá tên Tailscale
TAILSCALE=1 ./scripts/k8s-expose.sh                                     # forward + tailscale serve
```
Thành viên kết nối Kafka bằng `<tên-máy>.<tailnet>.ts.net:30090,...:30091,...:30092`.

**Cách K8s-native, dùng Tailscale operator (chưa chạy thử):**
```bash
TS_CLIENT_ID=... TS_CLIENT_SECRET=... TAILNET=tailXXXX.ts.net ./scripts/k8s-tailscale.sh
```
Script cài operator bằng Helm, gắn annotation lên Service để mỗi broker, PostgreSQL, Grafana và Kafka UI có một tên riêng trong tailnet. Không cần để máy mở cửa sổ port-forward. Điều kiện cần ghi ở đầu script.

## 5. Kiểm tra hoạt động

```bash
kubectl -n kol-system logs deploy/social-collectors --tail=20
kubectl -n kol-system logs deploy/stream-processor --tail=20
kubectl -n kol-system logs deploy/metrics-writer --tail=20
kubectl cnpg status kol-db -n kol-system        # nếu đã cài plugin cnpg; không thì: kubectl get cluster -n kol-system
python processor/view_leaderboard.py            # cần k8s-expose.sh đang chạy
```

Grafana có hai dashboard: **KOL Platform - Kafka Cluster & Load** (giám sát) và **KOL Leaderboard** (bảng xếp hạng KOL, từ khóa, điểm theo thời gian, đọc từ PostgreSQL).

Truy vấn database:
```bash
kubectl -n kol-system exec -it kol-db-1 -- psql -U postgres -d kol \
  -c "SELECT kol_id, handle, engagement_score FROM kol.kol_scores ORDER BY engagement_score DESC LIMIT 10;"
```
Dùng pod primary: `kubectl -n kol-system get pods -l cnpg.io/instanceRole=primary`.

## 6. Chịu lỗi và tự phục hồi

**Broker Kafka chết:**
```bash
kubectl -n kol-system delete pod kafka-1
kubectl -n kol-system get pods -w
```
StatefulSet tạo lại `kafka-1` với cùng node id và cùng PVC. Trong lúc đó producer `acks=all` vẫn ghi được vì còn 2/3 bản sao.

**PostgreSQL primary chết:**
```bash
kubectl -n kol-system get pods -l cnpg.io/instanceRole=primary
kubectl -n kol-system delete pod <pod-primary>
kubectl -n kol-system get cluster kol-db -w
```
CloudNativePG nâng một replica lên làm primary và chuyển `kol-db-rw` sang đó. `db-writer` và `metrics-writer` tự kết nối lại; offset Kafka chỉ được commit sau khi PostgreSQL đã lưu, nên không mất dữ liệu.

**Module 3 bị khởi động lại hoặc scale:** consumer group rebalance; pod nhận partition mới đọc điểm lũy kế của KOL từ `kol.kol_scores` nên điểm không bị tính lại từ 0.

## 7. Auto-scaling

Cần metrics-server: `minikube addons enable metrics-server`.

```bash
kubectl -n kol-system get hpa                    # stream-processor: 1–6 pod theo CPU 70%
kubectl apply -k k8s/apps                        # load test + HPA demo
kubectl -n kol-system set env deploy/loadgen-producer LOADGEN_RATE=5000
kubectl -n kol-system get hpa -w
```

Số pod Module 3 tối đa có ích là 9, bằng tổng partition của `kol.raw.bluesky` (6) và `kol.raw.youtube` (3). Muốn scale theo consumer lag thay vì CPU, xem `k8s/addons/keda/`.

## 8. Gỡ bỏ

```bash
kubectl delete -k k8s/apps
kubectl delete namespace kol-system      # xóa cả PVC: mất dữ liệu Kafka và PostgreSQL
minikube delete                          # xóa luôn cluster
```
