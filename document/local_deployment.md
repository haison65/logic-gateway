# Local deployment (host + scripts)

SoT: `scripts/*.ps1`, `scripts/_common.ps1` (`NodeDefs`), `configs/local/*`.  
Docker load: [docker.md](docker.md) + `.\scripts\start.ps1`. Quick start: [guide_setup.md](guide_setup.md).

---

## Topology

### `start_local.ps1` (headless host)

```text
master     :9200
http2gw    :8080 / UDP :9000
logic-1    UDP :9100   (node_id=2)
logic-2    UDP :9101   (node_id=3)
logic-3    UDP :9102   (node_id=4)
```

Configs: `configs/local/*.dev.yaml`. PID/log: `scripts/run/`.

### `start_local_p0.ps1` (cửa sổ debug)

Chỉ **master + http2gw + logic-1 + logic-2** (không logic-3).

### Docker local

master + http2gw + logic×3 + prometheus; clients qua profile `load` — xem [docker.md](docker.md).

---

## Scripts

| Script | Việc |
|--------|------|
| `start.ps1` | Wrapper → `start_load.ps1` (Docker load) |
| `load_local.ps1` | Alias → `start_load.ps1` |
| `start_load.ps1` | Up Compose local + chạy N client profile `load` |
| `start_local.ps1` | `go run` 5 process host (có logic-3) |
| `start_local_p0.ps1` | Cửa sổ; 2 Logic |
| `load_local_p0.ps1` | Host: 2 http2client + 4 perf |
| `stop_local.ps1` | Kill theo `pids.json` |
| `restart_local.ps1` | stop + start_local |
| `status_local.ps1` | PID, healthz/ready, `/v1/nodes` |
| `kill_node.ps1 -Name <node>` | Kill một trong: master, http2gw, logic-1, logic-2, logic-3 |

---

## Smoke + failover (host)

```powershell
.\scripts\start_local.ps1
.\scripts\status_local.ps1
go run ./cmd/client -n 1 -unique-session -body hello

.\scripts\kill_node.ps1 -Name logic-1
# Đợi HB → DEAD (~3–6s); request mới qua Logic còn ACTIVE
go run ./cmd/client -n 20 -c 4 -unique-session -body hello

.\scripts\stop_local.ps1
```

---

## Docker load

```powershell
# configs/local/load.request.yaml
.\scripts\start.ps1
.\scripts\start.ps1 -Duration 3m -Clients 2
```

Artefacts: `scripts/run-logs/`, `scripts/fail-logs/`.

---

## Observability

| Endpoint | Ý nghĩa |
|----------|---------|
| `:8080/healthz` `/ready` | GW |
| `:8080/metrics` `/metrics.json` | GW Prometheus / JSON |
| `:9191` `:9192` `:9193` `/metrics` | Logic (Docker publish) |
| `:9200/healthz` `/v1/nodes` | Master |
| `:9090` | Prometheus UI |

---

## Timeout

| Lớp | Ví dụ | Hết hạn |
|-----|--------|---------|
| Client | `10s` (`load.request.yaml` / `-timeout`) | Client fail |
| GW transaction | `5000ms` (`http2gw*.yaml`) | HTTP **504** |

---

## Runtime invariants

- Gateway có một UDP `Receive` consumer; `dispatch` demux envelope.
- Router chỉ chọn node; không gửi UDP.
- Transaction chỉ quản lý correlation, timeout, response, và tối đa một failover.
- Logic đọc UDP, đưa DATA vào bounded worker queue, rồi trả `DATA_RESPONSE` hoặc `ERROR`.
- Registry là in-memory; restart Gateway mất node state.

## HA

Application-level Active-Active + 1 failover retry + DEAD filter.  
Không phải HA đa host hạ tầng.
