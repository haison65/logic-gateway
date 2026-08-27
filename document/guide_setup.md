# Hướng dẫn dựng môi trường và chạy E2E

Tài liệu **quick start** local. Topology đầy đủ (Master, 2 Logic, scripts, failover): [`local_deployment.md`](local_deployment.md).  
Docker / Compose: [`docker.md`](docker.md). Resource limits: [`resource_model.md`](resource_model.md).  
Prometheus + report: [`huong_dan_prometheus_report.md`](huong_dan_prometheus_report.md).  
**Nếu lệch code, lấy code làm chuẩn.** README cũng mô tả API/kiến trúc.

---

## Luồng cơ bản

```text
Client (cmd/client, HTTP/2 h2c)
        │  POST /v1/data
        ▼
http2gw (TCP :8080, UDP :9000)
        │  UDP protobuf DATA_REQUEST
        ▼
Logic   (UDP :9100 / :9101, echo 1001/1002/1003)
        │  DATA_RESPONSE + transaction_id
        ▼
http2gw → HTTP/2 200 + body → Client
```

Không cần `http.remote` cho vòng echo này. `http.remote` chỉ cho chiều Logic → GW → HTTP remote (mục 8).

Mọi lệnh từ root repo (`d:\Golang\project\logic-gateway`), PowerShell.

---

## 1. Yêu cầu

- Go trên `PATH` (`go version`)
- `proto/gen/go/` đã có trong repo
- Cổng trống (topology tối thiểu / đầy đủ):

| Process | Bind (local) | Vai trò |
|---------|----------------|---------|
| master | `127.0.0.1:9200` | Control-plane inventory (tuỳ chọn khi dùng `configs/local`) |
| http2gw | `127.0.0.1:8080` | HTTP/2 h2c |
| http2gw | `127.0.0.1:9000` | UDP REGISTER / HEARTBEAT / DATA |
| logic-1 | `127.0.0.1:9100` | Logic `node_id=2` |
| logic-2 | `127.0.0.1:9101` | Logic `node_id=3` (topology đầy đủ) |

```powershell
netstat -ano | findstr "8080 9000 9100 9101 9200"
```

---

## 2. Cách nhanh nhất — topology đầy đủ (khuyến nghị)

```powershell
.\scripts\start_local.ps1
.\scripts\status_local.ps1

go run ./cmd/client -n 1 -unique-session -body hello

.\scripts\load_local.ps1
.\scripts\stop_local.ps1
```

Script dùng `configs/local/*` (Master + GW + 2 Logic). Chi tiết lệnh / failover drill: [`local_deployment.md`](local_deployment.md).

Window debug (mỗi process một cửa sổ): `.\scripts\start_local_p0.ps1`.

---

## 3. Test E2E tự động (không cần chạy binary)

```powershell
go test ./internal/logicnode -run "TestHTTPClientEchoViaLogic|TestE2EFailoverWhenLogicMarkedDead" -v
```

- `TestHTTPClientEchoViaLogic` — 1 Logic, HTTP/2 + echo + `X-Transaction-Id`
- `TestE2EFailoverWhenLogicMarkedDead` — 2 Logic, một node bị loại, request mới vẫn 200

---

## 4. E2E tay — tối thiểu (1 GW + 1 Logic)

Thứ tự: **http2gw → logic → client**.

**Terminal 1 — http2gw**

```powershell
go run ./cmd/http2gw -config configs/http2gw.dev.yaml
```

Log cần: UDP `127.0.0.1:9000`, HTTP `127.0.0.1:8080`. `-debug=false` để giảm log khi tải.

**Terminal 2 — logic**

```powershell
go run ./cmd/logic -config configs/logic.dev.yaml
```

Log cần: `logic đã đăng ký`. Đợi ~1s để HEARTBEAT → **ACTIVE** (trước đó client có thể nhận **503**).

**Terminal 3 — client**

```powershell
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -session-id sess-1 -body hello
```

Kỳ vọng: `proto: HTTP/2`, `status: 200`, `body: hello`, có `X-Transaction-Id`.

`message-id` hợp lệ: `1001` / `1002` / `1003`. Id khác → thường HTTP **502**.

### 2 Logic tay (không script)

```powershell
go run ./cmd/http2gw -config configs/local/http2gw.dev.yaml
go run ./cmd/logic -config configs/local/logic-1.dev.yaml
go run ./cmd/logic -config configs/local/logic-2.dev.yaml
# (tuỳ chọn) go run ./cmd/master -config configs/local/master.dev.yaml
go run ./cmd/client -n 1 -unique-session -body hello
```

---

## 5. Đẩy tải

GW + Logic đang chạy:

```powershell
go run ./cmd/client -n 100 -c 10
go run ./cmd/client -n 1000 -c 50 -unique-session
go run ./cmd/client -d 10s -c 20 -qps 100 -v

# Identity YAML (2 Http2-client + 4 Performance)
.\scripts\load_local.ps1
.\scripts\load_local.ps1 -Duration 60s
```

| Cờ | Mặc định | Ý nghĩa |
|----|----------|---------|
| `-addr` | `http://127.0.0.1:8080` | http2gw |
| `-config` | (trống) | YAML LoadClient (`configs/local/perf-*.yaml`, …) |
| `-message-id` | `1001` | `X-Message-Id` |
| `-session-id` | `sess-1` | Consistent-hash |
| `-unique-session` | `false` | Trải nhiều Logic |
| `-n` / `-c` / `-d` / `-qps` | `1` / `1` / `0` / `0` | Số request / concurrency / duration / trần QPS |
| `-v` | `false` | In từng lỗi |

`-n 1 -c 1` (không `-d`): in chi tiết một response. Load: in ok/fail/rps + latency percentiles.

---

## 6. Luồng một request (tóm tắt)

```text
cmd/client  --HTTP/2 POST /v1/data-->  http2gw :8080
http2gw     --UDP DATA_REQUEST------>  Logic ACTIVE
logic       --UDP DATA_RESPONSE----->  http2gw :9000
http2gw     --HTTP/2 200 + body----->  cmd/client
```

http2gw: Create `transaction_id` → Route (ACTIVE Logic + message_id) → UDP Send → Wait → Complete.  
Có failover tối đa 1 lần (Send fail / timeout) — exclude node vừa fail.  
Logic: REGISTER → HEARTBEAT → ACTIVE; echo 1001/1002/1003.

---

## 7. Lỗi thường gặp

| Hiện tượng | Xử lý |
|------------|--------|
| `connection refused` | GW chưa chạy / sai `-addr` |
| `bind: address already in use` | Cổng bị chiếm — `stop_local` hoặc tắt process cũ |
| HTTP **503** | Logic chưa ACTIVE / cả pool DEAD |
| HTTP **504** | Timeout chờ DATA_RESPONSE |
| HTTP **502** | `message-id` lạ / UDP lỗi |
| HTTP **400** | Thiếu `X-Message-Id` |
| Master không thấy node | Chưa set `master_url` trong YAML / Master chưa listen `:9200` |

---

## 8. Health và metrics

```powershell
curl.exe -s http://127.0.0.1:8080/healthz
curl.exe -s http://127.0.0.1:8080/ready
curl.exe -s http://127.0.0.1:8080/metrics | findstr http2gw_
curl.exe -s http://127.0.0.1:8080/metrics.json

# Khi chạy Master
curl.exe -s http://127.0.0.1:9200/healthz
curl.exe -s http://127.0.0.1:9200/v1/nodes
```

- `/metrics` — Prometheus (`http2gw_*`, gồm `logic_nodes{state=...}`)
- `/metrics.json` — snapshot JSON (request, fail reason, latency, UDP/HB counters)

Chi tiết PromQL / checklist report: [`huong_dan_prometheus_report.md`](huong_dan_prometheus_report.md).

---

## 9. Chiều Logic → HTTP/2 remote (không bắt buộc)

Logic gửi `DATA_REQUEST` UDP → http2gw gọi HTTP/2 tới `http.remote` → trả `DATA_RESPONSE` / ERROR về Logic.

- `configs/http2gw.dev.yaml` thường **để trống** `remote` → outbound nhận `NO_ROUTING_TARGET`
- Request: `POST {remote}/v1/data` + header message/session/trace/transaction

Không cần mục này để chạy echo local với `cmd/client`.

---

## 10. Docker

Xem [`docker.md`](docker.md).

```powershell
# Tối thiểu: 1 GW + 1 Logic + Prometheus
docker compose up --build -d

# Đầy đủ + CPU/mem limit: Master + GW + 2 Logic + Prometheus
docker compose -f docker-compose.local.yml up -d --build

go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -unique-session -body hello
```
