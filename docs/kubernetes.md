# Triển khai trên Kubernetes

## 1. Thành phần

```
namespace kol-system
├── StatefulSet kafka (3 pod: kafka-0, kafka-1, kafka-2)  + PVC data-kafka-N
├── Service kafka-headless (DNS từng pod, publishNotReadyAddresses)
├── Service kafka (bootstrap ClusterIP :9092)
├── Service kafka-{0,1,2}-external (NodePort 30090-30092)
├── PodDisruptionBudget kafka (maxUnavailable 1)
├── Job kafka-topics-init (tạo topic từ config/topics.conf)
├── Deployment kafka-ui, kafka-exporter, prometheus (+ RBAC), grafana
└── k8s/apps: Deployment loadgen-producer, loadgen-consumer + HPA
```

| File | Nội dung |
|---|---|
| `k8s/base/kafka/statefulset.yaml` | Broker KRaft; node id = số thứ tự pod; voter list được dựng từ `KOL_KAFKA_REPLICAS` |
| `k8s/base/kafka/services.yaml` | Headless + bootstrap + NodePort từng broker |
| `k8s/base/kustomization.yaml` | Gom manifest, sinh ConfigMap từ `config/`, `scripts/`, `monitoring/` |
| `k8s/overlays/dev/` | Heap 512 MB, PVC 2 Gi cho cluster local |
| `k8s/apps/` | Load generator + HPA, template cho module khác |
| `k8s/addons/keda/` | Scale consumer theo lag (tùy chọn) |

## 2. Chuẩn bị cluster local

| Cluster | Ghi chú |
|---|---|
| **Docker Desktop** → Settings → Kubernetes → Enable | Đơn giản nhất. Dùng chung image với Docker, NodePort mở trên `localhost`. |
| **minikube** `minikube start --cpus 4 --memory 8g` | `minikube addons enable metrics-server` cho HPA. Truy cập NodePort bằng `minikube ip`. |
| **kind** `kind create cluster` | Phải `kind load docker-image`. NodePort cần `extraPortMappings`. |

Cần khoảng 4 CPU / 6–8 GB RAM cho cả cụm.

## 3. Triển khai

```bash
./scripts/build-images.sh                 # hoặc: ./scripts/build-images.sh minikube | kind
./scripts/k8s-deploy.sh                   # render overlay dev + apply + chờ Kafka & topic job
kubectl -n kol-system get pods,pvc,svc
```

`k8s/base` dùng file nằm ngoài thư mục `k8s/` (topics, alert rules, dashboard), vì vậy phải render bằng `kubectl kustomize --load-restrictor LoadRestrictionsNone`. `kubectl apply -k k8s/overlays/dev` sẽ báo lỗi; hãy dùng script. Xem manifest sẽ được apply: `./scripts/k8s-deploy.sh dev --dry`.

Truy cập:

```bash
kubectl -n kol-system port-forward svc/grafana 3000:3000
kubectl -n kol-system port-forward svc/prometheus 9090:9090
kubectl -n kol-system port-forward svc/kafka-ui 8080:8080
```

Kafka từ máy host: `localhost:30090,localhost:30091,localhost:30092` (Docker Desktop). Với minikube, sửa env `KOL_EXTERNAL_HOST` trong StatefulSet thành địa chỉ `minikube ip`.

> `kubectl port-forward` **không** dùng được cho Kafka, vì client sẽ nhận địa chỉ advertised của từng broker sau lần kết nối đầu tiên. Dùng NodePort hoặc chạy client trong cluster.

## 4. Auto-healing

```bash
# Xóa một broker: StatefulSet tạo lại kafka-1 với cùng node id và cùng PVC
kubectl -n kol-system delete pod kafka-1
kubectl -n kol-system get pods -w

# Kiểm tra quorum và ISR trong lúc đó
kubectl -n kol-system exec kafka-0 -- env KAFKA_HEAP_OPTS=-Xmx256m \
  /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
kubectl -n kol-system exec kafka-0 -- env KAFKA_HEAP_OPTS=-Xmx256m \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic kol.loadtest
```

Trên Grafana: `Brokers up` giảm 3 → 2, `Under-replicated partitions` > 0, rồi tự về 0 khi pod chạy lại. Producer `acks=all` vẫn ghi được trong suốt quá trình.

`PodDisruptionBudget` chặn `kubectl drain` làm chết 2 broker cùng lúc.

## 5. Auto-scaling

### HPA theo CPU (mặc định)

```bash
kubectl apply -k k8s/apps
kubectl -n kol-system set env deploy/loadgen-producer LOADGEN_RATE=5000   # tăng tải
kubectl -n kol-system get hpa loadgen-consumer -w                         # replica tăng dần tới 6
```

`loadgen-consumer` mô phỏng xử lý tốn CPU (`LOADGEN_PROCESS_DELAY_MS`). Khi tải tăng, CPU vượt 60% và HPA thêm pod. Consumer group tự rebalance chia lại 6 partition. `maxReplicas` = số partition của topic, vì consumer dư sẽ không có partition để đọc.

Docker Desktop không có sẵn metrics-server:
```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml
kubectl -n kube-system patch deploy metrics-server --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
```

### KEDA theo consumer lag (khuyến nghị cho stream processing)

CPU không phải lúc nào cũng phản ánh đúng tình trạng "không theo kịp" (consumer có thể chờ I/O). Lag mới là chỉ số trực tiếp. Xem hướng dẫn trong `k8s/addons/keda/scaledobject-consumer-lag.yaml`.

### Scale Kafka

- Thêm broker: tăng `replicas` **và** `KOL_KAFKA_REPLICAS`. Lưu ý: voter list tĩnh nên thay đổi số controller cần làm theo quy trình reconfiguration của KRaft. Với đồ án, giữ cố định 3 node.
- Thêm partition: sửa `config/topics.conf`, chạy lại `./scripts/k8s-deploy.sh`.

## 6. Gỡ bỏ

```bash
kubectl delete -k k8s/apps
kubectl delete namespace kol-system      # xóa cả PVC → mất dữ liệu Kafka
```
