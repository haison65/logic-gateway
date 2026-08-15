# logic-gateway

HTTP/2 gateway that communicates with Logic service instances over UDP and Protocol Buffers.

## Status

Phase 8 — `cmd/http2gw` và `cmd/logic` chạy được. Logic REGISTER + HEARTBEAT + echo `DATA_RESPONSE`.

Hướng dẫn dựng môi trường và E2E: [document/guide_setup.md](document/guide_setup.md).

## Build

```bash
go build ./...
```

Or:

```bash
make build
```

## Protobuf

Regenerate Go types from `proto/*.proto`:

```bash
make proto
```

Requirements:

- [`protoc`](https://github.com/protocolbuffers/protobuf/releases) on `PATH`
- `protoc-gen-go` (installed by `make proto-tools`)

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
```

Windows (without `make`):

```powershell
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
New-Item -ItemType Directory -Force proto/gen/go | Out-Null
protoc --proto_path=proto --go_out=proto/gen/go --go_opt=paths=source_relative `
  proto/common.proto proto/register.proto proto/heartbeat.proto proto/data.proto proto/error.proto proto/envelope.proto
```

Do not edit files under `proto/gen/go/` by hand.

## Binaries

| Command | Description |
|---------|-------------|
| `go run ./cmd/http2gw -config configs/http2gw.dev.yaml` | HTTP/2 gateway (UDP + HTTP) |
| `go run ./cmd/logic -config configs/logic.dev.yaml` | Logic: REGISTER, HEARTBEAT, echo DATA |
| `go run ./cmd/client` | Client HTTP/2 (h2c); `-n`/`-c`/`-d` để đẩy tải |

Chạy khép vòng (ba terminal, gateway trước):

```powershell
go run ./cmd/http2gw -config configs/http2gw.dev.yaml
go run ./cmd/logic -config configs/logic.dev.yaml
```

Đợi Logic ACTIVE (~1s), rồi:

```powershell
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -session-id sess-1 -body hello
```

Đẩy tải (một kết nối HTTP/2, nhiều stream):

```powershell
go run ./cmd/client -n 1000 -c 50
go run ./cmd/client -d 10s -c 100 -unique-session
go run ./cmd/client -n 5000 -c 50 -qps 200
```

- UDP gateway: `127.0.0.1:9000`
- UDP logic: `127.0.0.1:9100` (`node_id: 2`)
- HTTP: `http://127.0.0.1:8080`
- `GET /healthz`
- `GET /metrics` — số request, in-flight, latency min/max/avg (ms)
- `POST /v1/data` — header bắt buộc `X-Message-Id` (1001/1002/1003), tùy chọn `X-Session-Id`, `X-Trace-Id`

## Layout

```text
cmd/                  application entry points
internal/transport/udp UDP + protobuf transport
proto/                Protocol Buffer definitions
proto/gen/go/         generated Go types (do not edit)
configs/              YAML configuration skeletons
```

## UDP transport tests

```bash
go test ./internal/transport/udp/
```
