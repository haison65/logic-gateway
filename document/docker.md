# Docker (local deployment)

Không phải production-ready. E2E local Compose: hai container trên mạng nội bộ, client HTTP/2 chạy **trên host**.

SoT hành vi: `HTTP2GW_Logic_UDP_Protobuf_Design.md`. Localhost không Docker: `guide_setup.md`.

------------------------------------------------------------------------

## Local (`*.dev.yaml`) — không dùng Compose

Bind `127.0.0.1`. Thứ tự: http2gw → logic → client.

```powershell
go run ./cmd/http2gw -config configs/http2gw.dev.yaml
go run ./cmd/logic -config configs/logic.dev.yaml
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -session-id sess-1 -body hello
```

------------------------------------------------------------------------

## Docker Compose

File: `docker-compose.yml`. Image: `Dockerfile` (target `http2gw` / `logic`). Config trong image: `configs/http2gw.docker.yaml`, `configs/logic.docker.yaml` (listen `0.0.0.0`; `gateway.host: http2gw`; `node.ip: logic`).

**Không** mount `*.dev.yaml` vào container.

Mạng: `logic-gateway-app`. DNS: `http2gw`, `logic`.

### Ports

| Cổng | Ở đâu | Publish ra host |
|------|--------|-----------------|
| TCP 8080 | http2gw HTTP/2 | **Có** `8080:8080` |
| UDP 9000 | http2gw (REGISTER / HB / DATA) | **Không** — chỉ mạng Compose |
| UDP 9100 | logic DATA | **Không** |

8080 trên máy phải trống (`go run` local hoặc container lẻ).

### Start / stop

```powershell
docker compose config
docker compose up --build -d
docker compose logs -f
docker compose stop
docker compose down
```

`stop` gửi SIGTERM: gateway `Shutdown` HTTP + đóng UDP (~5s). Registry in-memory **mất** khi restart — đúng hành vi hiện tại.

Compose mặc định `-debug=false` (tránh log từng DATA làm trễ HEARTBEAT khi đẩy tải). Bật lại: sửa `command` trong compose.

### Test HTTP/2 (host)

Đợi log Logic `logic đã đăng ký`, ~1s (ACTIVE):

```powershell
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -session-id sess-1 -body hello
```

Kỳ vọng: `proto: HTTP/2`, `status: 200`, `body: hello`.

Health GW từ **host** (không có curl trong distroless):

```powershell
curl.exe -s http://127.0.0.1:8080/healthz
```

Logic **không** có HTTP health — không thêm server chỉ để Docker healthcheck. Xác nhận: log `logic đã đăng ký` / `container Up`.

### Logs

```powershell
docker compose logs http2gw
docker compose logs logic
docker compose logs -f --tail 100
```

### Tải

Nhẹ: `go run ./cmd/client -n 200 -c 10`.  
Nặng (`-n 5000 -c 50`) với HB timeout 3s/6s + một Logic: dễ **SUSPECT/DEAD** → HTTP 503. DEAD không hồi bằng heartbeat; `docker compose restart logic` rồi đợi REGISTER lại.

------------------------------------------------------------------------

## Troubleshooting

| Hiện tượng | Hướng xử lý |
|------------|-------------|
| DNS không resolve / Logic `resolve host "http2gw"` | Phải `docker compose up`, không `docker run` lẻ. Tên service đúng `http2gw` / `logic`. |
| REGISTER fail / spam `gửi lại REGISTER` | GW chưa listen UDP 9000; sai `gateway.host`; hai stack khác mạng. |
| Logic không ACTIVE / HTTP 503 | Chưa HB (~1s); node DEAD sau tải — `restart logic`; Router chỉ chọn ACTIVE. |
| UDP không tới / 504 | Advertise `127.0.0.1` hoặc `0.0.0.0` (docker yaml không được vậy); firewall; Logic không echo. |
| Port conflict `8080` | `netstat -ano \| findstr ":8080"`; tắt `go run` / container cũ. |
| Container restart | Registry trống; Logic phải REGISTER lại (retry sẵn). |
| Client `connection refused` | Compose chưa up; sai `-addr`; bind chưa `0.0.0.0` trong docker yaml. |
| `dead node cannot heartbeat` | Monitor đã DEAD; HB bị từ chối; restart Logic. |

------------------------------------------------------------------------

## Image (tham khảo)

Multi-stage `golang:1.26` → `distroless/static-debian12:nonroot`. Không root. Không health binary trong image.
