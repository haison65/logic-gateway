# Hướng dẫn test + thu thập Prometheus cho report

Ngày: 2026-08-27  
Mục tiêu: chạy tải có kiểm soát, scrape Prometheus, lấy số liệu viết báo cáo hiệu năng / ổn định.

---

## 1. Metrics đã có

| Endpoint | Format | Dùng khi |
|----------|--------|----------|
| `GET /metrics` | Prometheus / OpenMetrics | Scrape bằng Prometheus, PromQL, Grafana |
| `GET /metrics.json` | JSON snapshot | Kiểm tra nhanh bằng curl / script |

### Metric ứng dụng (prefix `http2gw_`)

| Metric | Loại | Ý nghĩa cho report |
|--------|------|--------------------|
| `http2gw_http_requests_total{status,result}` | Counter | Tổng request; tách OK/fail và HTTP status |
| `http2gw_http_requests_fail_total{status,reason}` | Counter | Fail theo reason (`timeout`, `no_routing_target`, …) |
| `http2gw_http_requests_in_flight` | Gauge | Concurrent đang xử lý |
| `http2gw_http_request_duration_seconds` | Histogram | Latency → rate, p50/p95/p99 |
| `http2gw_logic_registered_total` | Counter | Logic REGISTER accepted |
| `http2gw_heartbeat_success_total` | Counter | HB Logic→GW OK |
| `http2gw_heartbeat_timeout_total` | Counter | Monitor timeout (SUSPECT/DEAD) |
| `http2gw_udp_rx_total` / `http2gw_udp_tx_total` | Counter | UDP vào/ra dispatcher |
| `http2gw_route_failed_total` | Counter | Không route được Logic |
| `http2gw_transaction_timeout_total` | Counter | Chờ DATA_RESPONSE quá hạn |
| `go_*` / `process_*` | runtime | Goroutine, GC, RSS (bối cảnh hệ thống) |

---

## 2. Chuẩn bị môi trường

### Cách A0 — P0 Local: 1 HTTP2GW + 2 Logic (`go run` + script)

Topology lab Local (không cần K8s):

| Role | Số | Cách chạy |
|------|---:|-----------|
| HTTP2GW | 1 | `configs/http2gw.dev.yaml` — `:8080` / UDP `:9000` |
| Logic | 2 | `logic-1.dev.yaml` (`node_id=2`, UDP `9100`) + `logic-2.dev.yaml` (`node_id=3`, UDP `9101`) |
| Http2 client | 2 | process `cmd/client` (script) |
| Performance | 4 | process `cmd/client` (script) |

```powershell
# Mo 3 cua so: http2gw, logic-1, logic-2
.\scripts\start_local_p0.ps1

# Smoke (sau khi ca hai Logic log "logic da dang ky")
curl.exe -s http://127.0.0.1:8080/healthz
go run ./cmd/client -n 1 -unique-session -body hello

# Day tai: 2 client + 4 performance (mac dinh 30s)
.\scripts\load_local_p0.ps1
.\scripts\load_local_p0.ps1 -Duration 60s -ClientQPS 20 -PerfQPS 50
```

Log client: `scripts/load-logs/`. Dùng `-unique-session` để `consistent_hash` trải 2 Logic.

Prometheus (tuỳ chọn): `docker compose up -d prometheus` → UI `:9090`, target `http2gw-host` UP.

### Cách A — Local 1 Logic (`go run`) + Prometheus Docker

Terminal 1–2:

```powershell
go run ./cmd/http2gw -config configs/http2gw.dev.yaml
go run ./cmd/logic   -config configs/logic.dev.yaml
```

Terminal 3 — chỉ Prometheus (không build lại app):

```powershell
docker compose up -d prometheus
```

Prometheus UI: http://127.0.0.1:9090  
Job `http2gw-host` scrape `host.docker.internal:8080`.

Kiểm tra target: **Status → Targets** — `http2gw-host` phải **UP**.

### Cách B — Full Compose (http2gw + logic + prometheus)

```powershell
docker compose up -d --build
```

- HTTP gateway: http://127.0.0.1:8080  
- Prometheus: http://127.0.0.1:9090  
- Job `http2gw` scrape `http2gw:8080` trong mạng Compose.

Client vẫn chạy **trên host** (h2c):

```powershell
go run ./cmd/client -addr http://127.0.0.1:8080 -n 1 -body hello
```

---

## 3. Smoke test (trước khi đo tải)

```powershell
# Health
curl.exe -s http://127.0.0.1:8080/healthz

# Một request thành công
go run ./cmd/client -n 1 -message-id 1001 -body hello

# Prometheus text (phải thấy http2gw_*)
curl.exe -s http://127.0.0.1:8080/metrics | findstr http2gw_

# JSON (tuỳ chọn)
curl.exe -s http://127.0.0.1:8080/metrics.json
```

Ghi vào report: thời điểm bắt đầu, phiên bản commit (`git rev-parse --short HEAD`), máy (CPU/RAM), OS.

---

## 4. Kịch bản test đề xuất (để viết report)

Chạy lần lượt; **ghi lại** lệnh, thời gian bắt đầu/kết thúc, output client.

| # | Tên | Lệnh gợi ý | Mục đích |
|---|-----|------------|----------|
| T1 | Baseline | `-n 100 -c 1` | Latency đơn luồng, không queue |
| T2 | Concurrent | `-n 2000 -c 20` | Throughput + p95 dưới tải vừa |
| T3 | Sustained | `-d 60s -c 20 -qps 100` | Ổn định 60s, RPS cố định |
| T4 | Stress | `-d 30s -c 50 -qps 500` | Đẩy cao; quan sát timeout / fail |
| T5 | Fail path | Client **không** `-message-id` hoặc dừng Logic | `fail_total`, `route_failed` |

Ví dụ T3:

```powershell
go run ./cmd/client -d 60s -c 20 -qps 100 -message-id 1001 -body hello
```

Sau mỗi kịch bản, copy block stdout client (ok/fail/rps/latency p50/p95/p99) vào report.

---

## 5. PromQL — số liệu chụp cho report

Trong Prometheus UI → Graph (hoặc API). Đặt **time range** khớp cửa sổ test.

### Throughput (req/s)

```promql
sum(rate(http2gw_http_requests_total[1m]))
```

### Error rate (%)

```promql
100 * sum(rate(http2gw_http_requests_total{result="fail"}[1m]))
  / sum(rate(http2gw_http_requests_total[1m]))
```

### Latency p50 / p95 / p99 (giây)

```promql
histogram_quantile(0.50, sum(rate(http2gw_http_request_duration_seconds_bucket[1m])) by (le))
histogram_quantile(0.95, sum(rate(http2gw_http_request_duration_seconds_bucket[1m])) by (le))
histogram_quantile(0.99, sum(rate(http2gw_http_request_duration_seconds_bucket[1m])) by (le))
```

### Fail theo reason

```promql
sum by (reason) (increase(http2gw_http_requests_fail_total[5m]))
```

### Timeout / route fail trong cửa sổ test

```promql
increase(http2gw_transaction_timeout_total[5m])
increase(http2gw_route_failed_total[5m])
```

### Heartbeat & REGISTER

```promql
increase(http2gw_logic_registered_total[1h])
rate(http2gw_heartbeat_success_total[1m])
increase(http2gw_heartbeat_timeout_total[5m])
```

### In-flight & runtime

```promql
http2gw_http_requests_in_flight
go_goroutines
process_resident_memory_bytes
```

### Export số liệu qua API (PowerShell)

Thay `QUERY` và khoảng thời gian:

```powershell
$q = [uri]::EscapeDataString('sum(rate(http2gw_http_requests_total[1m]))')
curl.exe -s "http://127.0.0.1:9090/api/v1/query?query=$q"
```

Instant query tại một thời điểm; cho time series:

```powershell
$q = [uri]::EscapeDataString('histogram_quantile(0.95, sum(rate(http2gw_http_request_duration_seconds_bucket[1m])) by (le))')
$start = [DateTimeOffset]::UtcNow.AddMinutes(-5).ToUnixTimeSeconds()
$end   = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
curl.exe -s "http://127.0.0.1:9090/api/v1/query_range?query=$q&start=$start&end=$end&step=5"
```

Lưu JSON response vào thư mục report (ví dụ `report/raw/t3_p95.json`).

---

## 6. Checklist nội dung report

1. **Môi trường:** OS, CPU, RAM, cách chạy (local / Compose), commit hash.  
2. **Topology:** http2gw `:8080` + Logic UDP + client h2c.  
3. **Kịch bản:** bảng T1–T5 + tham số `-n/-c/-d/-qps`.  
4. **Kết quả client:** ok, fail, rps, latency min/avg/p50/p95/p99/max.  
5. **Kết quả Prometheus:** throughput, error %, p50/p95/p99, fail-by-reason.  
6. **Ổn định:** `heartbeat_timeout`, `transaction_timeout`, `route_failed`, goroutine/RSS.  
7. **Nhận xét:** điểm bão hòa (khi nào p99/fail tăng), giới hạn (1 Logic echo, không DB).  
8. **Phụ lục:** lệnh đầy đủ, screenshot Targets UP, mẫu `/metrics` hoặc JSON.

---

## 7. Unit / integration test (CI local)

```powershell
go test ./...
go test ./internal/metrics ./internal/httpsrv -count=1
```

Không thay thế load test; chỉ xác nhận dual-write metrics + handler `/metrics` / `/metrics.json`.

---

## 8. Dừng / dọn

```powershell
docker compose down
# giữ volume Prometheus:
# docker compose down   (volume prometheus_data vẫn còn)
# xóa luôn dữ liệu scrape:
docker compose down -v
```

---

## 9. Ghi chú

- Scrape Prometheus dùng HTTP/1.1 tới cùng cổng h2c — bình thường.  
- `http2gw` job trong Compose có thể **DOWN** nếu bạn chỉ chạy `go run` (dùng job `http2gw-host`).  
- Retention mặc định Compose: **7 ngày** (`--storage.tsdb.retention.time=7d`).  
- Client stats và Prometheus histogram có thể lệch nhẹ (cửa sổ rate, đồng hồ) — report nên nêu cả hai nguồn.
