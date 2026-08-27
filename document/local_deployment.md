# Local deployment (Phase 9–12)

## Topology

```text
Master :9200
HTTP2GW :8080 / UDP :9000
Logic-1 UDP :9100
Logic-2 UDP :9101
(+ Http2-client ×2, Performance ×4 khi load)
```

## Commands

| Script | Việc |
|--------|------|
| `.\scripts\start_local.ps1` | Start headless + `scripts/run/pids.json` + logs |
| `.\scripts\stop_local.ps1` | Stop tree theo PID |
| `.\scripts\restart_local.ps1` | stop + start |
| `.\scripts\status_local.ps1` | PID / health / Master inventory |
| `.\scripts\kill_node.ps1 -Name logic-1` | Kill 1 node (failover drill) |
| `.\scripts\load_local.ps1` | 2 client + 4 perf (`configs/local/*.yaml`) |
| `.\scripts\start_local_p0.ps1` | Window mode (debug) |

## Smoke + failover drill

```powershell
.\scripts\start_local.ps1
.\scripts\status_local.ps1
go run ./cmd/client -n 1 -unique-session -body hello

# Failover drill
.\scripts\kill_node.ps1 -Name logic-1
# Đợi HB timeout (~3–6s) hoặc Master thấy DEAD; request mới vẫn OK qua logic-2
go run ./cmd/client -n 20 -c 4 -unique-session -body hello

.\scripts\stop_local.ps1
```

## Docker + resource limits

```powershell
docker compose -f docker-compose.local.yml up -d --build
docker stats --no-stream
curl.exe -s http://127.0.0.1:9200/v1/nodes
```

Xem `document/resource_model.md` (GOMAXPROCS ≠ cgroup).

## Observability

| Endpoint | Ý nghĩa |
|----------|---------|
| `GET :8080/healthz` `/ready` | GW liveness/readiness |
| `GET :8080/metrics` | Prometheus (`http2gw_logic_nodes{state=...}`, …) |
| `GET :8080/metrics.json` | JSON snapshot |
| `GET :9200/healthz` `/ready` `/v1/nodes` | Master control-plane |

## HA note

Local = **application-level** Active-Active + failover (retry Send/timeout + DEAD filter).  
**Không** phải infrastructure HA đa host.
