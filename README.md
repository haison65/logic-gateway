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
configs/              YAML (*.dev.yaml cho local)
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

## Docker Compose (HTTP2GW + Logic)

Chi tiết: [document/docker.md](document/docker.md). Tóm tắt:

```powershell
docker compose up --build -d
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -session-id sess-1 -body hello
docker compose down
```

Publish host chỉ **8080/tcp**. UDP 9000/9100 nội bộ. Localhost không Docker: `*.dev.yaml` + `go run`.