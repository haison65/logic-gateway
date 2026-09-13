# logic-gateway

Gateway HTTP/2 h2c nhận `POST /v1/data`, định tuyến request tới Logic qua UDP + protobuf `Envelope`, rồi trả response HTTP.

Go module: `github.com/haison65/logic-gateway` (**Go 1.26**).  
**SoT:** code trong repo. Tài liệu lệch code → lấy code.

```text
cmd/client  --HTTP/2 POST /v1/data-->  http2gw :8080
http2gw     --UDP DATA_REQUEST------>  logic (UDP :9100/:9101/:9102)
logic       --UDP DATA_RESPONSE----->  http2gw :9000
http2gw     --HTTP status + body---->  cmd/client

(optional) agents --HTTP--> master :9200
(metrics)  prometheus <-- scrape -- http2gw:8080 , logic*:9190
```

| Tài liệu | Nội dung |
|----------|----------|
| [document/guide_setup.md](document/guide_setup.md) | Quick start (host + Docker load) |
| [document/docker.md](document/docker.md) | Compose tối thiểu / local + client load |
| [document/local_deployment.md](document/local_deployment.md) | Scripts host + failover drill |
| [document/resource_model.md](document/resource_model.md) | CPU/mem Compose vs GOMAXPROCS vs YAML metadata |
| [document/huong_dan_prometheus_report.md](document/huong_dan_prometheus_report.md) | Metrics + PromQL |
| [document/HTTP2GW_Logic_UDP_Protobuf_Design.md](document/HTTP2GW_Logic_UDP_Protobuf_Design.md) | Thiết kế (ý đồ); đối chiếu mục Metrics với code |

---

## Process

| Process | Binary | Vai trò |
|---------|--------|---------|
| HTTP2GW | `cmd/http2gw` | HTTP/2 + UDP DATA-plane: registry, route, transaction, dispatch |
| Logic | `cmd/logic` | UDP worker: REGISTER/HB, echo `1001`–`1003`, DATA worker pool, optional `/metrics` |
| Master | `cmd/master` | Control-plane HTTP `:9200` (không trên đường DATA) |
| Client | `cmd/client` | Test/tải h2c (host hoặc Compose profile `load`) |

**Bất biến runtime GW:** router không `Send` UDP; transaction không `Receive`; **một** `dispatch` consumer `Receive`.

---

## Packages chính (`internal/`)

| Package | Việc |
|---------|------|
| `gateway` / `logicnode` / `master` | Lắp process |
| `httpsrv` | h2c: `/v1/data`, health, `/metrics`, `/metrics.json` |
| `transaction` | Create → Route → Send → Wait; failover tối đa 1 lần |
| `router` | ACTIVE + capability; `consistent_hash` / `round_robin`; `RouteExcluding` |
| `dispatch` | Demux UDP |
| `transport/udp` | Codec + Send/Receive; request socket buffer 32 MiB, OS may clamp |
| `registration` / `registry` | REGISTER; state RAM |
| `heartbeat` | Handle / Monitor / Sender |
| `metrics` | Dual-write Prometheus + JSON |
| `controlplane` / `outbound` / `config` | Agent Master; `http.remote`; YAML |

---

## DATA path

```text
POST /v1/data
  → Create(transaction_id)
  → Route (Logic ACTIVE + message_id)
  → UDP Send
  → Wait(DATA_RESPONSE)  ★ hết transaction.timeout → 504
  → (Send fail | timeout) → tối đa 1 lần RouteExcluding → Send/Wait lại
  → Complete → HTTP status/body (+ X-Transaction-Id khi có)
```

| Timeout | Nguồn điển hình | Hết hạn |
|---------|-----------------|---------|
| Client HTTP | `-timeout` / `load.request.yaml` → **10s** | Client fail |
| GW transaction | `transaction.timeout` → **5000ms** | HTTP **504** |

Logic: một goroutine `Receive`; **DATA_REQUEST** chạy trong worker pool (`cpu_cores×256`, clamp 128–2048; mặc định 512).

---

## HTTP API

### http2gw

| Method | Path |
|--------|------|
| `POST` | `/v1/data` (`X-Message-Id` bắt buộc; optional `X-Session-Id`, `X-Trace-Id`; body ≤ 1 MiB) |
| `GET` | `/healthz`, `/health`, `/ready` |
| `GET` | `/metrics`, `/metrics.json` |

Lỗi thường: **400**, **413**, **499**, **502**, **503**, **504**.

### master `:9200`

`/healthz`, `/health`, `/ready`, `GET /v1/nodes`, `POST /v1/register`, `POST /v1/heartbeat`, `POST /v1/nodes/{id}/leave`.

### logic (khi `metrics.port` > 0)

`GET /metrics` — `logic_requests_total{node,node_id}`.

---

## Metrics (tên trong code)

**GW** (`http2gw_`):  
`http_requests_total`, `http_requests_fail_total`, `http_requests_in_flight`, `http_request_duration_seconds`,  
`logic_registered_total`, `heartbeat_success_total`, `heartbeat_timeout_total`,  
`udp_rx_total`, `udp_tx_total`, `route_failed_total`, `transaction_timeout_total`,  
`logic_nodes{state}`, `failover_retry_total`, **`logic_routed_total{node_id,node}`**,  
+ `go_*` / `process_*`.

**Logic** (`logic_`): **`logic_requests_total`**, + `go_*` / `process_*`.

Scrape: `deploy/prometheus/prometheus.yml` (jobs `http2gw`, `http2gw-host`, `logic`, `logic-single`).

---

## Capacity (code hiện tại)

| Mục | Giá trị |
|-----|---------|
| H2 `MaxConcurrentStreams` | **1024** |
| Client H2 connections / host | **4** (round-robin) |
| UDP `SO_RCVBUF`/`SO_SNDBUF` | **32 MiB request**; actual OS value may be lower |
| Logic DATA workers | **512** mặc định; docker `cpu_cores: 2` → 512 |
| Failover | 1 primary + **1** retry |

---

## Cấu hình & ID

Không overlay env cho YAML app (trừ `GOMAXPROCS` trong Compose). Dùng `-config`.

| ID | Role |
|----|------|
| 1 | master |
| 2, 3, 4 | logic-1, logic-2, logic-3 |
| 10 | http2gw |
| 20–25 | http2client-1…6 (Docker) |
| 20–22 | http2client-1…3 (dev host) |
| 30–33 | perf-1…4 (dev host) |

Cổng: Master **9200**, GW TCP **8080** / UDP **9000**, Logic UDP **9100/9101/9102**, Prometheus **9090**, Logic metrics host **9191/9192/9193** → container **9190**.

`resource.*` trong YAML = **metadata**. Limit thật = Compose `cpus` / `mem_limit` / `cpuset`.

---

## Chạy nhanh

### Docker load (đo hiệu năng)

```powershell
docker compose -f docker-compose.local.yml up -d --build
# Chỉnh configs/local/load.request.yaml rồi:
.\scripts\start.ps1
.\scripts\start.ps1 -Duration 3m -Clients 3 -ClientCPUs 2
```

`start.ps1` / `load_local.ps1` → `start_load.ps1`: up server, build client, `compose run` profile `load` (tối đa 6 client).  
Artefacts: `scripts/run-logs/`, `scripts/fail-logs/`.

Dừng:

```powershell
docker compose -f docker-compose.local.yml down
```

### Host (3 Logic)

```powershell
.\scripts\start_local.ps1
.\scripts\status_local.ps1
go run ./cmd/client -n 1 -unique-session -body hello
.\scripts\stop_local.ps1
```

`start_local_p0.ps1` = cửa sổ debug, **chỉ logic-1 + logic-2**.

### E2E tối thiểu

```powershell
go run ./cmd/http2gw -config configs/http2gw.dev.yaml
go run ./cmd/logic -config configs/logic.dev.yaml
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -body hello
```

### Test

```powershell
go test ./...
go test ./internal/logicnode -run "TestHTTPClientEchoViaLogic|TestE2EFailoverWhenLogicMarkedDead" -v
```

### Client flags (rút gọn)

`-config`, `-addr`, `-message-id`, `-session-id`, `-unique-session`, `-trace-id`, `-body`,  
`-timeout`, `-n`, `-c`, `-d`, `-qps`, `-v`, `-fail-log`, `-fail-log-max`,  
`-summary-log`, `-client-name`, `-client-cpus`, `-master-url`.

---

## Docker (tóm tắt)

| File | Topology |
|------|----------|
| `docker-compose.yml` | http2gw + **1** logic + prometheus (không CPU limit; logic metrics `:9190`) |
| `docker-compose.local.yml` | master + http2gw(**1 CPU**) + logic×3(**2 CPU**) + prometheus + client-1…6 (**profile `load`**) |

Chi tiết: [document/docker.md](document/docker.md).

---

## Tính năng và giới hạn

**Có:** h2c DATA path, registry + heartbeat, capability-aware routing, failover 1 lần, bounded UDP queues, UDP send batch, Logic worker pool, admission `max_rps`/`max_rps_burst`/`max_pending`, packet/buffer pooling, Prometheus GW+Logic, JSON metrics, Docker load, pprof, Envelope `ERROR` → transaction completion.

**Phase sau (chưa SoT):**
- **P1:** bật `http.remote` outbound trên môi trường thật (code + test đã có; local thường `remote: ""`)
- **P2:** multi-HTTP2GW cluster
- **P3:** Logic business thật (hiện echo 1001–1003), auth, registry bền vững, phân mảnh UDP, CI GitHub Actions

Registry RAM mất khi restart GW.

---

## Lỗi thường gặp

| Hiện tượng | Nguyên nhân |
|------------|-------------|
| connection refused :8080 | GW chưa chạy |
| 429 | Vượt admission limit `max_rps` hoặc `max_pending` |
| 503 | Không Logic ACTIVE hoặc Logic overload |
| 504 | Hết `transaction.timeout` chờ DATA_RESPONSE (hoặc Logic ERROR code TIMEOUT) |
| 400 / 503 / 502 từ Logic ERROR | Envelope `MESSAGE_TYPE_ERROR` đã Complete waiter (không còn 504 giả) |
| 400 / 413 | Thiếu `X-Message-Id` / body > 1 MiB |
| Spam REGISTER | GW chưa listen UDP |
| `resolve host "http2gw"` | YAML docker ngoài mạng Compose |
| `start_load: N client fail` | Có HTTP fail (thường 504) — xem summary |
| Compose `run --cpus` unknown | Compose v5 — dùng override YAML của `start_load.ps1` |
