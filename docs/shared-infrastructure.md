# Hạ tầng dùng chung của nhóm

Kafka và PostgreSQL chạy trên **máy của một thành viên** (gọi là máy chủ). Các thành viên khác kết nối qua mạng riêng [Tailscale](https://tailscale.com), nên dữ liệu cào được nằm ở một chỗ và cả nhóm cùng dùng.

```
Máy thành viên (collector, consumer, API)
        │  Tailscale: mã hóa, chỉ thành viên trong nhóm vào được
        ▼
Máy chủ ── tailscale serve ──► 127.0.0.1 ──► Kafka x3, PostgreSQL, Kafka UI, Grafana, Prometheus
```

Mọi cổng chỉ mở trên `127.0.0.1`, người cùng mạng wifi với máy chủ không truy cập được. Kafka và Kafka UI không có mật khẩu, nên lớp bảo vệ của chúng là Tailscale: **chỉ mời thành viên trong nhóm vào tailnet**.

## 1. Dành cho thành viên

### Kết nối

1. Nhận lời mời vào tailnet của nhóm và cài [Tailscale](https://tailscale.com/download).
2. Xin người giữ máy chủ thông tin kết nối (tên máy và mật khẩu). Không commit các thông tin này lên git.
3. Kiểm tra:

```bash
pip install confluent-kafka "psycopg[binary]"
python scripts/check_shared.py \
  --kafka HOST:9092,HOST:9093,HOST:9094 \
  --postgres postgresql://kol_app:MAT_KHAU@HOST:5432/kol
```

`HOST` có dạng `ten-may.tailXXXX.ts.net`. Kết quả mong đợi là `OK` cho cả Kafka và PostgreSQL.

### Địa chỉ

| Dịch vụ | Địa chỉ |
|---|---|
| Kafka | `HOST:9092,HOST:9093,HOST:9094` |
| PostgreSQL | `HOST:5432`, database `kol`, schema `kol` |
| Kafka UI | `http://HOST:8080` |
| Grafana | `http://HOST:3000` |
| Prometheus | `http://HOST:9090` |

Nên đọc các địa chỉ này từ biến môi trường, để chuyển giữa local và dùng chung không phải sửa code. Cấu hình producer/consumer xem [integration-guide.md](integration-guide.md).

### Tài khoản PostgreSQL

| Tài khoản | Quyền | Dùng cho |
|---|---|---|
| `kol_app` | Tạo bảng trong schema `kol`, đọc và ghi | Module CSDL, service ghi dữ liệu |
| `kol_readonly` | Chỉ `SELECT` | API, dashboard, truy vấn thử |
| `kol_admin` | Toàn quyền | Chỉ người giữ máy chủ |

Bảng do `kol_app` tạo sẽ tự động đọc được bằng `kol_readonly`.

### Quy tắc khi dùng chung

- Thử nghiệm trên topic `kol.dev.sandbox`, không ghi dữ liệu thử vào `kol.raw.*`.
- Mỗi module dùng một `group.id` riêng. Dùng chung `group.id` sẽ làm hai bên chia nhau message và mỗi bên chỉ thấy một phần.
- Không chạy load test khi nhóm đang cào dữ liệu thật, vì tất cả dùng chung tài nguyên của một máy.
- Collector phải chịu được việc máy chủ tạm tắt. Mặc định message chưa gửi được chỉ chờ 5 phút (`message.timeout.ms`), sau đó báo lỗi qua `on_delivery` và **bị bỏ** nếu code không xử lý.

## 2. Dữ liệu được giữ bao lâu

| Nơi lưu | Thời hạn |
|---|---|
| `kol.raw.bluesky` | 3 ngày hoặc 12 GiB, tùy điều kiện nào tới trước |
| `kol.raw.youtube` | 7 ngày hoặc 1,5 GiB |
| PostgreSQL | Lâu dài, sao lưu mỗi ngày |

**Kafka chỉ là bộ đệm.** `db-writer` (thư mục [`dbwriter/`](../dbwriter)) đọc `kol.raw.*` và ghi từng event vào bảng `kol.social_events`. Service này được khai báo trong `docker-compose.shared.yml`, nên `./scripts/shared.sh up` bật luôn. Câu lệnh tạo bảng nằm trong [`dbwriter/schema.sql`](../dbwriter/schema.sql): PostgreSQL chạy file này khi volume còn trống, writer cũng chạy lại mỗi lần khởi động nên volume cũ vẫn có bảng mới.

Ghi là at-least-once: offset chỉ được commit sau khi transaction ghi PostgreSQL thành công, mỗi dòng là upsert theo `event_id` nên replay không tạo bản ghi trùng. Message không đọc được đẩy sang `kol.dlq` kèm lý do và payload gốc.

Kiểm tra dữ liệu đã vào DB:

```bash
# trạng thái writer và số dòng đã lưu
docker logs --tail=20 kol-db-writer
docker exec kol-postgres psql -U kol_admin -d kol \
  -c 'SELECT platform, count(*) FROM kol.social_events GROUP BY platform;'

# 5 bản ghi mới nhất
docker exec kol-postgres psql -U kol_admin -d kol \
  -c 'SELECT platform, event_type, kol_id, left(text, 60), created_at FROM kol.social_events ORDER BY created_at DESC LIMIT 5;'
```

Giới hạn dung lượng nằm trong [`config/topics.conf`](../config/topics.conf), tính theo từng partition của từng bản sao. Toàn bộ topic chiếm tối đa khoảng 60 GiB trên đĩa máy chủ.

## 3. Dành cho người giữ máy chủ

### Yêu cầu

- Docker cấp được khoảng 8 GB RAM, đĩa trống ít nhất 100 GB.
- Git Bash (trên Windows), Docker Desktop, Tailscale.
- Máy không ngủ khi cắm điện. Trên Windows: `powercfg /change standby-timeout-ac 0`, đồng thời chỉnh để gập máy không ngủ.
- Docker Desktop bật *Start when you sign in*. Sau khi máy khởi động lại, phải đăng nhập Windows thì Docker mới chạy.

### Thiết lập lần đầu

1. Đăng ký Tailscale, cài lên máy chủ và đăng nhập. Gói Personal miễn phí cho tối đa 6 người, đủ cho nhóm 5 người.
2. Mời các thành viên tại [trang Users](https://login.tailscale.com/admin/users).
3. Khởi động:

```bash
./scripts/shared.sh up
```

Script tự tạo `.env.shared` với mật khẩu ngẫu nhiên và tên Tailscale của máy, chạy toàn bộ dịch vụ rồi mở cổng trên Tailscale. Cuối cùng nó in thông tin kết nối để gửi cho nhóm qua kênh riêng.

### Vận hành

```bash
./scripts/shared.sh status   # container, dung lượng đĩa, backup, cổng Tailscale
./scripts/shared.sh info     # in lại thông tin kết nối
./scripts/shared.sh backup   # sao lưu PostgreSQL ngay
./scripts/shared.sh down     # dừng, giữ nguyên dữ liệu
```

Không chạy `docker compose down -v` trên máy chủ: lệnh này **xóa toàn bộ dữ liệu** Kafka và PostgreSQL.

### Sao lưu và khôi phục

Mỗi 24 giờ và mỗi lần service khởi động, PostgreSQL được dump vào thư mục `backups/`, giữ 14 ngày. Thư mục này nằm **trên cùng ổ đĩa** với dữ liệu, nên hỏng máy là mất cả hai. Hãy định kỳ chép `backups/` sang nơi khác, ví dụ Google Drive.

Khôi phục vào một database mới để kiểm tra trước (database `kol` đang chạy không bị ảnh hưởng):

```bash
export MSYS_NO_PATHCONV=1
docker exec kol-postgres psql -U kol_admin -d kol -c 'CREATE DATABASE restore_test'
docker exec -e PGPASSWORD=MAT_KHAU_ADMIN kol-postgres-backup \
  pg_restore -h postgres -U kol_admin -d restore_test /backups/TEN_FILE.dump
```

### Đổi mật khẩu

Mật khẩu trong `.env.shared` chỉ có tác dụng khi volume PostgreSQL được tạo lần đầu. Về sau, đổi bằng lệnh sau rồi cập nhật lại `.env.shared`:

```bash
MSYS_NO_PATHCONV=1 docker exec kol-postgres psql -U kol_admin -d kol \
  -c "ALTER ROLE kol_app PASSWORD 'MAT_KHAU_MOI'"
```

### Khi có người rời nhóm

Xóa người đó khỏi tailnet và đổi mật khẩu PostgreSQL.

### Nếu `tailscale serve` không hoạt động

Tài liệu của Tailscale không nói rõ về hỗ trợ trên Windows và phần này chưa được chạy thử. Nếu không dùng được, mở cổng trực tiếp trên địa chỉ Tailscale của máy (lấy bằng `tailscale ip -4`). Đặt trong `.env.shared`:

```
BIND_ADDRESS=100.x.y.z
```

Rồi chạy `NO_TAILSCALE=1 ./scripts/shared.sh up`. Khi đó Tailscale phải kết nối trước khi Docker khởi động, và có thể phải mở các cổng này trong Windows Firewall cho riêng mạng Tailscale. Tuyệt đối không dùng `0.0.0.0`.

## 4. Giới hạn của cách này

- **Một điểm lỗi duy nhất:** máy chủ tắt, ngủ hoặc mất mạng thì cả nhóm mất kết nối. Dữ liệu đã ghi vẫn còn khi máy chạy lại.
- **Ba broker trên một máy** chỉ chịu được lỗi của từng broker, không chịu được lỗi của máy. Đây là môi trường để nhóm cùng phát triển, kết quả load test trên đây không đại diện cho một cluster thật.
- **PostgreSQL chạy một node,** chưa có replication và failover. Phần chịu lỗi của CSDL thuộc module CSDL.
- **Kafka không phân quyền theo người:** ai trong tailnet cũng đọc, ghi và xóa được topic.

Khi cần chạy ổn định hơn (ví dụ giai đoạn demo), có thể chuyển sang VPS bằng cùng các lệnh trên, chỉ cần chuyển kèm dữ liệu.
