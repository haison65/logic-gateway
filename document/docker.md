# Docker (local deployment)

Không phải production-ready. Client HTTP/2 (`cmd/client`) chạy **trên host**.  
SoT hành vi: [`HTTP2GW_Logic_UDP_Protobuf_Design.md`](HTTP2GW_Logic_UDP_Protobuf_Design.md).  
Quick start không Docker: [`guide_setup.md`](guide_setup.md). Topology scripts: [`local_deployment.md`](local_deployment.md).  
Resource (GOMAXPROCS ≠ cgroup): [`resource_model.md`](resource_model.md).

---

## Hai file Compose

| File | Topology | Resource limit |
|------|----------|----------------|
| `docker-compose.yml` | http2gw + **1** logic + prometheus | Không |
| `docker-compose.local.yml` | **master** + http2gw + **logic-1** + **logic-2** + prometheus | Có (`cpus` / `mem_limit`) |

Image: `Dockerfile` targets `http2gw` / `logic` / `master` (distroless nonroot).

---

## A. Compose tối thiểu (`docker-compose.yml`)

- Config bake sẵn: `configs/http2gw.docker.yaml`, `configs/logic.docker.yaml`
- Mạng: `logic-gateway-app` — DNS `http2gw`, `logic`
- Listen `0.0.0.0`; `gateway.host=http2gw`; `node.ip=logic`

### Ports

| Cổng | Service | Publish host |
|------|---------|--------------|
| TCP 8080 | http2gw HTTP/2 + `/metrics` | Có |
| TCP 9090 | Prometheus UI | Có |
| UDP 9000 | http2gw | Không (chỉ mạng Compose) |
| UDP 9100 | logic | Không |

### Lệnh

```powershell
docker compose config
docker compose up --build -d
docker compose logs -f
docker compose stop
docker compose down
```

Mặc định `-debug=false`. Registry in-memory **mất** khi restart GW — Logic phải REGISTER lại (retry sẵn).

### Test từ host

Đợi log Logic `logic đã đăng ký`, ~1s (ACTIVE):

```powershell
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -session-id sess-1 -body hello
curl.exe -s http://127.0.0.1:8080/healthz
curl.exe -s http://127.0.0.1:8080/metrics | findstr http2gw_
```

Prometheus: http://127.0.0.1:9090 — target `http2gw` (compose) và/hoặc `http2gw-host` (khi scrape host). Chi tiết: [`huong_dan_prometheus_report.md`](huong_dan_prometheus_report.md).

### Logs / tải

```powershell
docker compose logs http2gw
docker compose logs logic
docker compose logs -f --tail 100

go run ./cmd/client -n 200 -c 10 -unique-session
```

Tải nặng + 1 Logic + HB timeout ngắn dễ **SUSPECT/DEAD** → 503. DEAD không hồi bằng HB: `docker compose restart logic`, đợi REGISTER.

---

## B. Compose đầy đủ + limits (`docker-compose.local.yml`)

Topology:

```text
master    :9200   (1 CPU, 512M)
http2gw   :8080   (4 CPU, 8G)   + UDP 9000 nội bộ
logic-1           (6 CPU, 8G)   UDP 9100, hostname logic-1
logic-2           (6 CPU, 8G)   UDP 9101, hostname logic-2
prometheus :9090
```

Config mount từ `configs/local/*.docker.yaml` (có `master_url: http://master:9200`).

```powershell
docker compose -f docker-compose.local.yml up -d --build
docker stats --no-stream
curl.exe -s http://127.0.0.1:9200/v1/nodes
curl.exe -s http://127.0.0.1:8080/healthz

go run ./cmd/client -config configs/local/perf-1.dev.yaml
.\scripts\load_local.ps1
```

**Yêu cầu Docker Desktop:** RAM VM khuyến nghị ≥ 24G nếu giữ `mem_limit` 8G×3; CPU time-slice nếu host < 17 core. Giảm `mem_limit`/`cpus` trong YAML nếu VM nhỏ hơn.

`GOMAXPROCS` trong compose = gợi ý scheduler Go — **không** thay cgroup limit. Xem [`resource_model.md`](resource_model.md).

Client / Performance **không** chạy trong Compose (tránh vượt RAM) — chạy trên host.

Dừng:

```powershell
docker compose -f docker-compose.local.yml down
# xóa volume Prometheus:
docker compose -f docker-compose.local.yml down -v
```

---

## Local không Docker (nhắc nhanh)

```powershell
.\scripts\start_local.ps1
# hoặc tối thiểu:
go run ./cmd/http2gw -config configs/http2gw.dev.yaml
go run ./cmd/logic -config configs/logic.dev.yaml
go run ./cmd/client -n 1 -body hello
```

---

## Troubleshooting

| Hiện tượng | Hướng xử lý |
|------------|-------------|
| DNS / `resolve host "http2gw"` | Phải `docker compose up`, không `docker run` lẻ; đúng tên service |
| Spam `gửi lại REGISTER` | GW chưa listen UDP 9000; sai `gateway.host`; khác mạng |
| HTTP 503 | Logic chưa ACTIVE / DEAD — `restart` logic tương ứng; Router chỉ ACTIVE |
| HTTP 504 | Advertise IP sai; Logic không echo; firewall |
| Port 8080/9200 conflict | Tắt `go run` / compose cũ: `netstat -ano \| findstr ":8080"` |
| Master `/v1/nodes` thiếu Logic | Dùng `docker-compose.local.yml` + yaml có `master_url`; đợi agent register |
| `docker stats` limit không đúng | Compose file cũ không có `mem_limit`; hoặc Docker Desktop RAM thấp |
| `dead node cannot heartbeat` | Restart Logic → REGISTER lại |
| Client `connection refused` | Compose chưa ready; bind chưa `0.0.0.0` trong docker yaml |

---

## Image (tham khảo)

Multi-stage `golang:1.26` → `gcr.io/distroless/static-debian12:nonroot`.  
Targets: `http2gw`, `logic`, `master`. Không root. Không shell/curl trong image — health từ **host**.
