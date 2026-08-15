# logic-gateway

HTTP/2 gateway nói chuyện với các instance Logic qua **UDP + Protocol Buffers**.

```text
cmd/client  --HTTP/2 POST /v1/data-->  http2gw :8080
http2gw     --UDP DATA_REQUEST------>  logic  :9100
logic       --UDP DATA_RESPONSE----->  http2gw :9000
http2gw     --HTTP/2 200 + body----->  cmd/client
```

Một datagram UDP = một Envelope protobuf. Một vòng `Receive` trên gateway. Logic local echo `message_id` **1001 / 1002 / 1003**.

Module: `github.com/haison65/logic-gateway` (Go 1.26).

## Tài liệu


| File                                                                                           | Nội dung                                      |
| ---------------------------------------------------------------------------------------------- | --------------------------------------------- |
| [document/HTTP2GW_Logic_UDP_Protobuf_Design.md](document/HTTP2GW_Logic_UDP_Protobuf_Design.md) | Thiết kế (SoT)                                |
| [document/docker.md](document/docker.md) | Compose, cổng, stop, troubleshooting |
| [document/guide_setup.md](document/guide_setup.md) | Dựng môi trường, E2E host, metrics |




## Nhanh (local)

Cổng trống: HTTP `127.0.0.1:8080`, UDP gateway `9000`, UDP logic `9100`. Chạy từ root repo, **gateway trước**.

```powershell
go run ./cmd/http2gw -config configs/http2gw.dev.yaml
go run ./cmd/logic -config configs/logic.dev.yaml
```

Đợi log `logic đã đăng ký` rồi ~1 giây (HEARTBEAT → ACTIVE):

```powershell
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -session-id sess-1 -body hello
```

Kỳ vọng: `proto: HTTP/2`, `status: 200`, `body: hello`, có `X-Transaction-Id`.

Đẩy tải (một kết nối HTTP/2, nhiều stream):

```powershell
go run ./cmd/client -n 1000 -c 50
go run ./cmd/client -d 10s -c 20 -unique-session
```

HTTP:


| Method | Đường dẫn  | Vai trò                                                               |
| ------ | ---------- | --------------------------------------------------------------------- |
| `POST` | `/v1/data` | Header bắt buộc `X-Message-Id`; tùy chọn `X-Session-Id`, `X-Trace-Id` |
| `GET`  | `/healthz` | Process sống                                                          |
| `GET`  | `/metrics` | JSON (request + bộ đếm design)                                        |


Vòng echo **không** cần `http.remote`. Trường đó chỉ khi Logic gửi `DATA_REQUEST` để gateway gọi HTTP/2 ra server khác.

## Test / build

```powershell
go test ./...
go vet ./...
go build ./...
```

E2E in-process (không chiếm 8080/9000/9100):

```powershell
go test ./internal/logicnode -run TestHTTPClientEchoViaLogic -v
```

`go test -race` cần CGO/gcc. Có GNU make: `make test`, `make vet`, `make build`.

## Protobuf

`proto/gen/go/` đã commit. Không sửa `*.pb.go` tay. Generate lại khi đổi `proto/*.proto`:

```powershell
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
New-Item -ItemType Directory -Force proto/gen/go | Out-Null
protoc --proto_path=proto --go_out=proto/gen/go --go_opt=paths=source_relative `
  proto/common.proto proto/register.proto proto/heartbeat.proto proto/data.proto proto/error.proto proto/envelope.proto
```

Hoặc `make proto` (cần `protoc` trên `PATH`).

## Cấu trúc

```text
cmd/http2gw/          gateway: HTTP/2 + UDP
cmd/logic/            Logic: REGISTER, HEARTBEAT, echo DATA
cmd/client/           client HTTP/2 (h2c) + đẩy tải
configs/              *.dev.yaml (host); *.docker.yaml (image)
Dockerfile            image http2gw + logic
docker-compose.yml    mạng DNS http2gw / logic
document/
internal/gateway/     wire http2gw
internal/httpsrv/     POST /v1/data, health, metrics
internal/dispatch/    consumer UDP Receive duy nhất
internal/transaction/ Create → Route → Send → Wait
internal/router/      RoundRobin / ConsistentHash
internal/registration/  REGISTER
internal/registry/    bảng node + state
internal/heartbeat/   ACTIVE / SUSPECT / DEAD
internal/logicnode/   vòng Logic
internal/outbound/    HTTP/2 client chiều Logic → remote
internal/transport/udp
proto/                .proto + gen/go
document/
```



## Binary


| Lệnh                                                    | Việc                                   |
| ------------------------------------------------------- | -------------------------------------- |
| `go run ./cmd/http2gw -config configs/http2gw.dev.yaml` | Gateway                                |
| `go run ./cmd/logic -config configs/logic.dev.yaml`     | Logic                                  |
| `go run ./cmd/client`                                   | Một request hoặc `-n`/`-c`/`-d`/`-qps` |


Ctrl+C: client → logic → http2gw. Cổng bị chiếm trên Windows: `netstat -ano | findstr "8080 9000 9100"` rồi `Stop-Process -Id <PID> -Force`.

## Docker

Không phải production-ready. Client **không** vào image: vẫn `go run ./cmd/client` trên host. Troubleshooting đầy đủ: [document/docker.md](document/docker.md).

### Topology

```text
Host  --TCP 8080-->  http2gw (container)
                       UDP :9000  (chỉ mạng Compose, DNS tên http2gw)
                         │
                         ▼
                       logic (container, UDP :9100, DNS tên logic)
```

Hai service trong `docker-compose.yml`, mạng `logic-gateway-app`. Logic `gateway.host: http2gw`, `node.ip: logic` (`configs/*.docker.yaml`). **Không** dùng `127.0.0.1` giữa hai container, **không** mount `*.dev.yaml`.

| File | Việc |
|------|------|
| `Dockerfile` | Multi-stage: `golang:1.26` → distroless **nonroot**; target `http2gw` / `logic` |
| `docker-compose.yml` | DNS, publish 8080, `-debug=false`, SIGTERM 10s |
| `configs/http2gw.docker.yaml` | Listen `0.0.0.0:8080` / UDP `9000` |
| `configs/logic.docker.yaml` | Listen `0.0.0.0:9100`, GW `http2gw:9000` |

### Cổng

| Cổng | Vai trò | Ra host? |
|------|---------|----------|
| TCP **8080** | HTTP/2 client | Có (`8080:8080`) |
| UDP **9000** | REGISTER / HEARTBEAT / DATA | Không |
| UDP **9100** | Logic nhận DATA | Không |

8080 trên máy phải trống. Tắt `go run` local hoặc container lẻ trước khi Compose.

### Chạy E2E

```powershell
docker compose config
docker compose up --build -d
docker compose logs -f
```

Đợi log Logic `logic đã đăng ký` rồi ~1s (HEARTBEAT → ACTIVE). Terminal khác:

```powershell
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -session-id sess-1 -body hello
```

Kỳ vọng: `proto: HTTP/2`, `status: 200`, `body: hello`.

Health GW từ host (image không có curl):

```powershell
curl.exe -s http://127.0.0.1:8080/healthz
```

Logic không có HTTP health — xem log REGISTER hoặc `docker compose ps`.

```powershell
docker compose logs http2gw
docker compose logs logic
docker compose stop
docker compose down
```

`stop` = SIGTERM: đóng HTTP + UDP. Registry RAM **mất** khi restart; Logic REGISTER lại (có retry).

Chỉ `docker run` một image, không mạng chung → client thường **503** (không có Logic ACTIVE).

Build image lẻ (không E2E):

```powershell
docker build --target http2gw -t logic-gateway/http2gw:local .
docker build --target logic -t logic-gateway/logic:local .
```

### Tải trên Compose

Nhẹ:

```powershell
go run ./cmd/client -n 200 -c 10
go run ./cmd/client -n 1000 -c 20 -qps 100
```

Nặng (`-n 5000 -c 50`) dễ làm Logic **SUSPECT/DEAD** (HB 3s/6s, một node) → **503**. DEAD không hồi bằng heartbeat:

```powershell
docker compose restart logic
```

Rồi đợi `logic đã đăng ký`. Compose mặc định `-debug=false` để log không chèn HB.

### Lỗi thường gặp

| Hiện tượng | Việc làm |
|------------|----------|
| `connection refused` | Compose chưa up; 8080 bị chiếm |
| `bind` / port in use | `netstat -ano \| findstr ":8080"` |
| `resolve host "http2gw"` | Phải Compose, không run lẻ |
| Spam `gửi lại REGISTER` | GW chưa listen UDP |
| HTTP **503** | Chưa ACTIVE, hoặc DEAD sau tải → `restart logic` |
| `dead node cannot heartbeat` | Restart Logic |
| HTTP **504** | UDP không echo / sai advertise IP |

Localhost không Docker: vẫn mục **Nhanh (local)** + `configs/*.dev.yaml`.