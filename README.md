# logic-gateway

Gateway **HTTP/2** chuyển payload nghiệp vụ tới các node **Logic** qua **UDP + Protocol Buffers**.

Prototype chạy được trên máy local và Docker: một (hoặc nhiều) process Logic echo `message_id` **1001 / 1002 / 1003**, gateway định tuyến Active-Active với **một lần failover** khi Send/timeout, kèm **Master** control-plane (inventory HTTP) và metrics **Prometheus**.

```text
cmd/client  --HTTP/2 h2c POST /v1/data-->  http2gw :8080
http2gw     --UDP DATA_REQUEST---------->  logic  :9100 / :9101
logic       --UDP DATA_RESPONSE--------->  http2gw :9000
http2gw     --HTTP 200 + body----------->  cmd/client

(optional)  http2gw / logic / client  --HTTP register/HB-->  master :9200
```

Module: `github.com/haison65/logic-gateway` (Go **1.26**).

| Tài liệu | Nội dung |
|----------|----------|
| [document/guide_setup.md](document/guide_setup.md) | Quick start local / E2E |
| [document/local_deployment.md](document/local_deployment.md) | Topology đầy đủ + scripts |
| [document/docker.md](document/docker.md) | Compose tối thiểu / đầy đủ |
| [document/resource_model.md](document/resource_model.md) | CPU/mem, GOMAXPROCS ≠ cgroup |
| [document/huong_dan_prometheus_report.md](document/huong_dan_prometheus_report.md) | Scrape, PromQL, checklist report |
| [document/HTTP2GW_Logic_UDP_Protobuf_Design.md](document/HTTP2GW_Logic_UDP_Protobuf_Design.md) | Ý đồ thiết kế |

**Nếu tài liệu và code khác nhau, lấy code làm chuẩn.**

---

## Dự án này làm gì

| | |
|---|---|
| **Vấn đề** | Mở HTTP/2 API đơn giản và bắc cầu từng request sang worker Logic UDP, dùng chung hợp đồng `Envelope`. |
| **Người dùng** | Developer chạy `http2gw`, `logic`, (tuỳ chọn) `master`, và `cmd/client` trên máy hoặc Docker Compose. |
| **Input** | `POST /v1/data`: header `X-Message-Id` (bắt buộc), tuỳ chọn `X-Session-Id` / `X-Trace-Id`, body ≤ 1 MiB. |
| **Output** | HTTP status từ `DataResponse.status` (echo dùng 200), body = payload, header `X-Transaction-Id` và `X-Message-Id`. |
| **Cách chạy** | Gateway gắn HTTP request với vòng UDP (`transaction_id`), route tới Logic **ACTIVE** có capability `message_id`, chờ `DATA_RESPONSE`; Send fail / timeout → tối đa **1** lần route lại (exclude node vừa fail). |

Không có database ứng dụng, không auth. Nghiệp vụ Logic hiện chỉ **echo**.

---

## Kiến trúc

### Process

| Process | Binary | Vai trò |
|---------|--------|---------|
| **HTTP2GW** | `cmd/http2gw` | HTTP/2 (h2c, TLS tuỳ chọn) + UDP. Registry DATA-plane, routing, transaction, dispatch. |
| **Logic** | `cmd/logic` | Worker UDP. REGISTER + HEARTBEAT tới gateway; echo DATA 1001–1003. |
| **Master** | `cmd/master` | Control-plane HTTP `:9200` — inventory node (không nằm trên đường DATA). |
| **Client** | `cmd/client` | Test / tải. Không đóng vào image Compose mặc định. |

**Hai mặt phẳng:**

- **DATA** — Client → GW → UDP Logic (không đổi mô hình thiết kế gốc).
- **Control** — Khi YAML có `master_url`, GW/Logic/client agent báo cáo Master qua HTTP (`internal/controlplane`). Master **không** thay UDP REGISTER của Logic.

### Gateway runtime

Khi `gateway.Run` start, các vòng chính chạy đến shutdown:

1. `heartbeat.Monitor` — SUSPECT / DEAD cho Logic.
2. `heartbeat.Sender` — UDP `HEARTBEAT_REQUEST` GW → Logic đã đăng ký.
3. `dispatch.Dispatcher` — **một** consumer duy nhất của `Transport.Receive`.
4. `httpsrv.Server` — TCP HTTP/2.
5. (Tuỳ chọn) control-plane agent → Master; poll registry metrics `http2gw_logic_nodes`.

Bất biến: **router không gửi UDP**; **transaction không Receive**; **dispatch là vòng Receive duy nhất** trên gateway.

```text
                    ┌─────────────────────────────────────────┐
                    │              http2gw                    │
 Client ─HTTP/2──►  │ httpsrv ─► transaction ─► router        │
                    │                 │                       │
                    │                 ▼                       │
                    │              udp.Send ──────────────────┼──► logic
                    │                 ▲                       │
                    │ dispatch.Receive┤                       │
                    │  REGISTER / HB / DATA_RESPONSE          │
                    └─────────────────────────────────────────┘
                              │ (optional master_url)
                              ▼
                           master :9200
```

**1 Logic node = 1 OS process** (không phải 1 goroutine trên gateway).

---

## Thành phần chính

| Package | Vai trò |
|---------|---------|
| `internal/gateway` | Lắp HTTP2GW: UDP, registry, heartbeat, router, transaction, dispatch, HTTP, agent Master. |
| `internal/logicnode` | Lắp Logic: REGISTER retry, HB, echo DATA, agent Master. |
| `internal/master` | HTTP control-plane: register / heartbeat / list nodes, monitor timeout. |
| `internal/controlplane` | Agent HTTP JSON tới Master. |
| `internal/httpsrv` | h2c/TLS: `/v1/data`, `/healthz`, `/health`, `/ready`, `/metrics`, `/metrics.json`. |
| `internal/transaction` | Create → Route → Send → Wait; failover tối đa 1 lần (`RouteExcluding`). |
| `internal/router` | Logic ACTIVE + capability; `consistent_hash` / `round_robin`; `RouteExcluding`. |
| `internal/dispatch` | Demux envelope UDP. |
| `internal/transport/udp` | Bind, protobuf codec, Send/Receive. |
| `internal/registration` / `registry` | Validate REGISTER; bảng node trong RAM. |
| `internal/heartbeat` | Handle (Logic→GW), Monitor, Sender (GW→Logic). |
| `internal/outbound` | `http.remote`: Logic `DATA_REQUEST` → HTTP/2 remote. |
| `internal/metrics` | Dual-write Prometheus + JSON. |
| `internal/config` | YAML + validate (kể cả `configs/local`, Master, LoadClient). |

---

## Luồng dữ liệu (tóm tắt)

### Đăng ký Logic → Gateway (UDP)

```text
REGISTER → REGISTERED → HEARTBEAT (Logic→GW) → ACTIVE
```

Chưa có Logic ACTIVE → `POST /v1/data` trả **503**.  
`DEAD` không hồi bằng HB — Logic phải **REGISTER lại**.

HTTP2GW cũng `registerSelf` **nội bộ** vào cùng registry (không phải mesh UDP sang Logic).

### DATA chiều vào

```text
POST /v1/data → Create(transaction_id) → Route → UDP Send → Wait
  → (Send fail | Wait timeout) → RouteExcluding(node fail) → Send/Wait lần 2
  → DATA_RESPONSE → Complete → HTTP status + body
```

### Heartbeat

| Chiều | Hiệu ứng |
|-------|----------|
| Logic → GW | Cập nhật registry → ACTIVE (quyết định route). |
| GW → Logic | Keep-alive; Logic echo response. Logic **không** theo dõi DEAD của gateway. |

### Control-plane (Master)

`POST` register + định kỳ heartbeat JSON → Master; `GET /v1/nodes` xem inventory. Không tham gia UDP DATA.

---

## HTTP API (http2gw)

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `POST` | `/v1/data` | Cầu UDP sang Logic |
| `GET` | `/healthz`, `/health` | Liveness |
| `GET` | `/ready` | Readiness (process sẵn sàng phục vụ) |
| `GET` | `/metrics` | Prometheus (`http2gw_*`) |
| `GET` | `/metrics.json` | Snapshot JSON |

**Lỗi chiều vào thường gặp:** 400 (thiếu/`X-Message-Id`), 413 (body > 1 MiB), 499 (client hủy), 502 (UDP/transport/message lạ), 503 (không Logic đủ điều kiện), 504 (timeout chờ response).

### Master (`:9200`)

| Endpoint | Mô tả |
|----------|-------|
| `GET /healthz`, `/health`, `/ready` | Health |
| `GET /v1/nodes` | Inventory |
| Register / heartbeat HTTP | Agent từ GW, Logic, client (khi có `master_url`) |

---

## Metrics

- **`GET /metrics`** — Prometheus (histogram latency, counters/gauges `http2gw_*`, gồm `logic_nodes{state=...}`, `failover_retry_total`, …).
- **`GET /metrics.json`** — cùng số liệu dạng JSON.

Compose có service **prometheus** (UI `:9090`). Chi tiết: [document/huong_dan_prometheus_report.md](document/huong_dan_prometheus_report.md).

---

## Cấu trúc thư mục

```text
.
├── cmd/
│   ├── http2gw/          Gateway
│   ├── logic/            Worker Logic
│   ├── master/           Control-plane
│   └── client/           Client tải / E2E
├── internal/             gateway, logicnode, master, controlplane, …
├── proto/                .proto + gen/go (đã commit)
├── configs/              *.dev.yaml, *.docker.yaml
│   └── local/            Master + 2 Logic + client/perf identities
├── scripts/              start/stop/status/load/kill_node (PowerShell)
├── document/             setup, docker, local deploy, prometheus, design
├── deploy/prometheus/    prometheus.yml
├── Dockerfile            targets: http2gw, logic, master
├── docker-compose.yml            1 GW + 1 Logic + Prometheus
├── docker-compose.local.yml      Master + GW + 2 Logic + limits + Prometheus
└── Makefile
```

---

## Điều kiện tiên quyết

- **Go 1.26** (`go.mod`)
- Tuỳ chọn: `protoc` + `protoc-gen-go` v1.36.12 để generate lại protobuf
- Tuỳ chọn: Docker Engine + Compose v2
- `go test -race` cần **CGO**

```bash
git clone https://github.com/haison65/logic-gateway.git
cd logic-gateway
go mod download
```

---

## Cấu hình

Không overlay biến môi trường. Đường dẫn từ **`-config`**. Thời lượng YAML dạng chuỗi (`1000ms`, `5s`).

| File / thư mục | Dùng cho |
|----------------|----------|
| `configs/http2gw.dev.yaml`, `logic.dev.yaml` | E2E tối thiểu (1 Logic) |
| `configs/local/*` | Topology đầy đủ: Master, GW, logic-1/2, http2client, perf |
| `configs/*.docker.yaml` | Bake/mount trong image Compose |

Cổng local điển hình: HTTP **8080**, UDP GW **9000**, Logic **9100** / **9101**, Master **9200**.

`http.remote` trong YAML đang ship thường **trống** → outbound Forward chưa cấu hình (không cần cho echo client→GW→Logic).

---

## Chạy nhanh

### Topology đầy đủ (khuyến nghị)

```powershell
.\scripts\start_local.ps1
.\scripts\status_local.ps1
go run ./cmd/client -n 1 -unique-session -body hello
.\scripts\load_local.ps1
.\scripts\stop_local.ps1
```

Chi tiết failover drill / kill node: [document/local_deployment.md](document/local_deployment.md).

### E2E tối thiểu (1 GW + 1 Logic)

```powershell
go run ./cmd/http2gw -config configs/http2gw.dev.yaml
go run ./cmd/logic -config configs/logic.dev.yaml
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -session-id sess-1 -body hello
```

### Test không cần binary ngoài

```powershell
go test ./internal/logicnode -run "TestHTTPClientEchoViaLogic|TestE2EFailoverWhenLogicMarkedDead" -v
```

### Docker

```powershell
# Tối thiểu
docker compose up --build -d

# Đầy đủ + CPU/mem limit
docker compose -f docker-compose.local.yml up -d --build

go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -unique-session -body hello
```

Chi tiết: [document/docker.md](document/docker.md).

### Client tải

| Cờ | Ý nghĩa |
|----|---------|
| `-addr`, `-message-id`, `-session-id`, `-unique-session` | Target và khóa route |
| `-config` | YAML LoadClient (`configs/local/perf-*.yaml`, …) |
| `-n` / `-c` / `-d` / `-qps` / `-v` | Số request, concurrency, duration, trần QPS, verbose |

```powershell
go run ./cmd/client -n 1000 -c 50 -unique-session
go run ./cmd/client -d 10s -c 20 -qps 100
```

---

## Kiểm thử

```bash
go test ./...
go vet ./...
go build ./...
# hoặc: make test / make vet / make build / make race
```

Generate protobuf: `make proto` (cần `protoc` trên `PATH`).

---

## Trạng thái hiện tại

### Đã có

- HTTP/2 h2c (+ TLS tuỳ chọn) `POST /v1/data`, health/ready, metrics Prometheus + JSON
- Envelope UDP; một Receive trên GW và trên Logic
- Logic REGISTER + HEARTBEAT; Monitor SUSPECT/DEAD
- Router consistent hash / round robin; **failover 1 lần** (Send fail / timeout)
- Master control-plane + agent (`master_url`)
- Scripts local + `docker-compose` / `docker-compose.local.yml` (resource limits)
- E2E in-process echo + failover

### Chưa / giới hạn

- Authn/authz; registry bền vững; cluster nhiều HTTP2GW
- Phân mảnh/ghép UDP; nghiệp vụ call thật (chỉ echo)
- GitHub Actions CI; đóng `cmd/client` vào image mặc định
- Local HA = **application-level** (Active-Active + failover), **không** phải HA đa host hạ tầng
- Envelope `ERROR` từ Logic không hoàn tất waiter HTTP → thường **504**
- Registry RAM mất khi restart GW — Logic phải REGISTER lại

---

## Quyết định thiết kế (theo code)

- **HTTP/2 h2c** — không fallback HTTP/1.1 trên đường DATA.
- **UDP + protobuf Envelope** — một datagram, một message.
- **Registry + heartbeat** — Logic tham gia động.
- **Transaction + channel** — khớp datagram với HTTP handler.
- **Một Receive** — tránh `ReadFrom` đồng thời trên cùng socket.
- **DATA vs control tách** — Master không nằm trên đường UDP nghiệp vụ.

---

## Xử lý lỗi thường gặp

| Hiện tượng | Nguyên nhân thường gặp |
|------------|------------------------|
| `connection refused` :8080 | http2gw chưa chạy |
| HTTP 503 | Logic chưa ACTIVE / pool DEAD |
| HTTP 504 | Không có `DATA_RESPONSE` đúng hạn |
| HTTP 400 / 413 | Thiếu `X-Message-Id` hoặc body quá lớn |
| Spam `gửi lại REGISTER` | Gateway chưa lắng UDP |
| `resolve host "http2gw"` | Dùng YAML docker ngoài DNS Compose |
| Master trống `/v1/nodes` | Chưa `master_url` / Master chưa listen |
| Prometheus target DOWN | Image cũ chưa rebuild sau đổi `/metrics` |
