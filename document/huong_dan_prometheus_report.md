# Prometheus — test và report

SoT metrics: `internal/metrics`, `internal/logicnode/metrics.go`, `deploy/prometheus/prometheus.yml`.

---

## 1. Endpoints

| URL | Format |
|-----|--------|
| `http://<gw>:8080/metrics` | Prometheus (`http2gw_*`) |
| `http://<gw>:8080/metrics.json` | JSON snapshot |
| `http://<logic>:9190/metrics` | Prometheus (`logic_*`) — khi bật `metrics.port` |

Host Docker local: Logic metrics `9191` / `9192` / `9193`.

---

## 2. Tên metric (code)

### GW — namespace `http2gw`

| Metric | Labels | Ý nghĩa |
|--------|--------|---------|
| `http2gw_http_requests_total` | `status`, `result` | Tổng request |
| `http2gw_http_requests_fail_total` | `status`, `reason` | Fail theo reason |
| `http2gw_http_requests_in_flight` | | Concurrent |
| `http2gw_http_request_duration_seconds` | | Latency histogram |
| `http2gw_logic_routed_total` | `node_id`, `node` | Route+Send OK theo Logic |
| `http2gw_logic_registered_total` | | REGISTER accepted |
| `http2gw_heartbeat_success_total` | | HB OK |
| `http2gw_heartbeat_timeout_total` | | Monitor timeout |
| `http2gw_udp_rx_total` / `http2gw_udp_tx_total` | | UDP dispatcher |
| `http2gw_route_failed_total` | | Không route |
| `http2gw_transaction_timeout_total` | | Wait quá hạn → 504 |
| `http2gw_logic_nodes` | `state` | Số node theo state |
| `http2gw_failover_retry_total` | | Retry sau Send failure hoặc transaction timeout |

+ `go_*`, `process_*`.

### Logic — namespace `logic`

| Metric | Labels | Ý nghĩa |
|--------|--------|---------|
| `logic_requests_total` | `node`, `node_id` (const) | DATA_REQUEST nhận được |
| `logic_register_status` | `node`, `node_id` | `1` = REGISTER accepted |
| `logic_heartbeat_rtt_seconds` | `node`, `node_id` | RTT heartbeat gần nhất |
| `logic_queue_size` | `node`, `node_id` | In-flight DATA |
| `logic_udp_data_request_rx_total` | `node`, `node_id` | DATA_REQUEST đọc từ UDP |
| `logic_udp_data_response_tx_total` | `node`, `node_id` | DATA/ERROR gửi về GW |
| `logic_processing_duration_seconds` | `node`, `node_id` | Latency xử lý trong worker |

---

## 3. Scrape jobs (`deploy/prometheus/prometheus.yml`)

| Job | Target |
|-----|--------|
| `http2gw` | `http2gw:8080` |
| `http2gw-host` | `host.docker.internal:8080` |
| `logic` | `logic-1:9190`, `logic-2:9190`, `logic-3:9190` |
| `logic-single` | `logic:9190` |

Interval: **5s**. Retention Compose: **7d**.

---

## 4. Chạy môi trường đo

### Docker local (đủ 3 Logic)

```powershell
docker compose -f docker-compose.local.yml up -d --build
# UI http://127.0.0.1:9090 — Targets: http2gw + logic UP

.\scripts\start.ps1 -Duration 60s
```

### Compose tối thiểu

```powershell
docker compose up -d --build
```

### go run + Prometheus

```powershell
go run ./cmd/http2gw -config configs/http2gw.dev.yaml
go run ./cmd/logic -config configs/logic.dev.yaml
docker compose up -d prometheus
```

Dùng job `http2gw-host`. Logic dev YAML mặc định **không** bật `metrics.port`.

---

## 5. Smoke

```powershell
curl.exe -s http://127.0.0.1:8080/healthz
go run ./cmd/client -n 1 -message-id 1001 -body hello
curl.exe -s http://127.0.0.1:8080/metrics | findstr "http2gw_logic_routed http2gw_http_requests"
curl.exe -s http://127.0.0.1:9191/metrics | findstr logic_requests
```

Ghi report: `git rev-parse --short HEAD`, tham số `load.request.yaml`, máy/VM.

---

## 6. Kịch bản gợi ý

| # | Cách | Mục đích |
|---|------|----------|
| T1 | `go run ./cmd/client -n 100 -c 1` | Baseline |
| T2 | `-n 2000 -c 20` | Concurrent |
| T3 | `-d 60s -c 20 -qps 100` | Sustained |
| T4 | `.\scripts\start.ps1 -Duration 1m` | Docker multi-client |
| T5 | PromQL by `node` sau T4 | Cân bằng router |
| T6 | Stop 1 Logic / thiếu message-id | 503 / 400 |

Artefacts T4: `scripts/run-logs/*_aggregate.json`, `scripts/fail-logs/*.jsonl`.

**Lưu ý timeout:** GW `transaction.timeout=5s` → 504; client `timeout=10s` là lớp khác.

---

## 7. PromQL

```promql
sum(rate(http2gw_http_requests_total[1m]))

100 * sum(rate(http2gw_http_requests_total{result="fail"}[1m]))
  / clamp_min(sum(rate(http2gw_http_requests_total[1m])), 1e-9)

histogram_quantile(0.95, sum(rate(http2gw_http_request_duration_seconds_bucket[1m])) by (le))

sum by (node) (increase(http2gw_logic_routed_total[5m]))
sum by (node) (increase(logic_requests_total[5m]))

increase(http2gw_transaction_timeout_total[5m])
sum by (reason) (increase(http2gw_http_requests_fail_total[5m]))

http2gw_http_requests_in_flight
go_goroutines
```

```powershell
$q = [uri]::EscapeDataString('sum by (node) (increase(http2gw_logic_routed_total[5m]))')
curl.exe -s "http://127.0.0.1:9090/api/v1/query?query=$q"
```

---

## 8. Checklist report

1. Môi trường + commit + Compose file  
2. Topology (GW CPU, số Logic, `client_count`×`concurrency`, body_size)  
3. Hai timeout (client vs transaction)  
4. Client aggregate (ok/fail/rps/latency)  
5. Prometheus: RPS, error %, p95, **phân bố node**  
6. `transaction_timeout`, `docker stats`  
7. Nhận xét bão hòa (p95 ≈ 5s + 504)  
8. Phụ lục Targets + mẫu metrics  

---

## 9. Test unit metrics

```powershell
go test ./internal/metrics ./internal/httpsrv ./internal/logicnode -count=1
```

---

## 10. Dọn

```powershell
docker compose -f docker-compose.local.yml down
docker compose down -v
```
