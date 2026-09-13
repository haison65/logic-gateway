# Hướng dẫn dựng môi trường và chạy

SoT: **code**. Liên quan: [docker.md](docker.md), [local_deployment.md](local_deployment.md), [resource_model.md](resource_model.md), [huong_dan_prometheus_report.md](huong_dan_prometheus_report.md).

Mọi lệnh từ root repo, PowerShell.

---

## Luồng

```text
Client (cmd/client, h2c, 4 conn/host)
  → POST /v1/data → http2gw :8080
  → UDP DATA_REQUEST → Logic ACTIVE (UDP 9100/9101/9102)
  → DATA_RESPONSE → http2gw :9000
  → HTTP response → Client
```

Echo `message_id` **1001 / 1002 / 1003**. Không cần `http.remote` cho vòng này.

---

## Yêu cầu

- Go **1.26** trên `PATH`
- `proto/gen/go/` đã có trong repo
- Tuỳ chọn: Docker Desktop + Compose v2

| Process | Cổng |
|---------|------|
| master | TCP 9200 |
| http2gw | TCP 8080, UDP 9000 |
| logic-1/2/3 | UDP 9100 / 9101 / 9102 |
| logic metrics (Docker) | host 9191/9192/9193 → 9190 |
| prometheus | TCP 9090 |

```powershell
netstat -ano | findstr "8080 9000 9100 9101 9102 9200 9090 9191"
```

---

## 1. Docker load (khuyến nghị đo tải)

Topology: **master + http2gw (1 CPU) + logic-1/2/3 (2 CPU) + prometheus**.  
Client: Compose profile **`load`** (không lên khi `up` thường).

```powershell
docker compose -f docker-compose.local.yml up -d --build

# Chỉnh configs/local/load.request.yaml (Ctrl+S) rồi:
.\scripts\start.ps1
.\scripts\start.ps1 -Duration 3m
.\scripts\start.ps1 -Clients 2 -ClientCPUs 2 -Duration 60s

# Alias cùng start_load:
.\scripts\load_local.ps1
```

| Nguồn | Ưu tiên |
|-------|---------|
| `-Clients` / `-ClientCPUs` / `-Duration` | Cao |
| `configs/local/load.request.yaml` | Trung |
| Mặc định script: 3 client × 2 CPU | Thấp |

`load.request.yaml` (mặc định hiện tại): `concurrency: 250`, `duration: 1m`, `timeout: 10s` (client), `body_size: 4096`, `unique_session: true`, `client_count: 3`.

**Hai timeout:**

| Lớp | Giá trị | Kết quả |
|-----|---------|---------|
| Client | `timeout` trong load YAML / `-timeout` | Client báo fail |
| GW | `transaction.timeout: 5000ms` trong `http2gw.docker.yaml` | HTTP **504** |

Artefacts:

- `scripts/run-logs/<stamp>_client-N.summary.json`, `*_aggregate.json`, `*_client-N.console.log`
- `scripts/fail-logs/client-N-fails.jsonl`
- Terminal: dòng client có prefix `[client-N]`

`exit=1` / `start_load: N client fail` = có request fail (thường 504), không nhất thiết script lỗi.

CPU client: `start_load.ps1` ghi override YAML — **không** dùng `docker compose run --cpus` (Compose v5 không có flag đó).

```powershell
docker compose -f docker-compose.local.yml down
```

Chi tiết: [docker.md](docker.md).

---

## 2. Host — 3 Logic (`start_local.ps1`)

```powershell
.\scripts\start_local.ps1
.\scripts\status_local.ps1
go run ./cmd/client -n 1 -unique-session -body hello
.\scripts\stop_local.ps1
```

PID/log: `scripts/run/`. Failover: [local_deployment.md](local_deployment.md).

Debug cửa sổ (**chỉ 2 Logic**): `.\scripts\start_local_p0.ps1`.  
Load host 2 client + 4 perf: `.\scripts\load_local_p0.ps1`.

---

## 3. Test in-process

```powershell
go test ./internal/logicnode -run "TestHTTPClientEchoViaLogic|TestE2EFailoverWhenLogicMarkedDead" -v
go test ./...
```

---

## 4. E2E tay — 1 GW + 1 Logic

```powershell
# T1
go run ./cmd/http2gw -config configs/http2gw.dev.yaml
# T2
go run ./cmd/logic -config configs/logic.dev.yaml
# T3 (sau "logic đã đăng ký" ~1s)
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -session-id sess-1 -body hello
```

Kỳ vọng: `HTTP/2`, `200`, body echo, có `X-Transaction-Id`.

---

## 5. Client trên host

```powershell
go run ./cmd/client -n 1000 -c 50 -unique-session
go run ./cmd/client -d 10s -c 20 -qps 100 -v
```

| Flag | Default | Ý nghĩa |
|------|---------|---------|
| `-addr` | `http://127.0.0.1:8080` | GW (+ `/v1/data`) |
| `-config` | | YAML LoadClient |
| `-message-id` | `1001` | |
| `-session-id` | `sess-1` | |
| `-unique-session` | `false` | Trải Logic |
| `-timeout` | `10s` | Timeout HTTP client |
| `-n` / `-c` / `-d` / `-qps` | `1`/`1`/`0`/`0` | Tải |
| `-fail-log` / `-summary-log` | | File observability |
| `-v` | `false` | |

Client dùng **4** HTTP/2 connection (round-robin).

---

## 6. Request path (tóm tắt code)

```text
httpsrv → transaction.Request
  Create → Route → UDP Send → Wait
  failover ≤ 1 (Send fail / DeadlineExceeded)
dispatch.Receive → OnResponse → Complete(transaction_id)
```

Logic `Receive` tuần tự; DATA vào **worker pool**.  
**504** = `Wait` hết `transaction.timeout`, không phải queue HTTP riêng.

---

## 7. Health / metrics

```powershell
curl.exe -s http://127.0.0.1:8080/healthz
curl.exe -s http://127.0.0.1:8080/metrics | findstr http2gw_
curl.exe -s http://127.0.0.1:8080/metrics.json
curl.exe -s http://127.0.0.1:9191/metrics | findstr logic_requests
curl.exe -s http://127.0.0.1:9200/v1/nodes
```

```promql
sum by (node) (increase(http2gw_logic_routed_total[5m]))
sum by (node) (increase(logic_requests_total[5m]))
increase(http2gw_transaction_timeout_total[5m])
```

---

## 8. Lỗi thường gặp

| Hiện tượng | Xử lý |
|------------|--------|
| connection refused | GW chưa chạy / sai `-addr` |
| bind in use | `stop_local` / `compose down` |
| **503** | Logic chưa ACTIVE |
| **504** | Hết transaction timeout — giảm `-c` / `concurrency`; xem PromQL timeout |
| **502** | message-id lạ / UDP send |
| **400** | Thiếu `X-Message-Id` |
| Docker Hub TLS timeout | Mạng registry — retry pull/build |
| `unknown flag: --cpus` | Đừng thêm vào `compose run`; dùng `start.ps1` |

---

## 9. Capacity trong code

| Mục | Giá trị |
|-----|---------|
| H2 MaxConcurrentStreams | 1024 |
| Client H2 conns | 4 |
| UDP socket buffer | 32 MiB request; OS may clamp actual value |
| Logic DATA workers | ≈ cpu_cores×256 (128–2048) |
| Docker local GW | 1 CPU / GOMAXPROCS=1 / 2G |
| Docker local Logic | 2 CPU / 2G mỗi node |

---

## 10. Outbound `http.remote` (tuỳ chọn)

Logic → GW → HTTP remote. YAML ship thường để trống `remote`. Không cần cho echo client→GW→Logic.
