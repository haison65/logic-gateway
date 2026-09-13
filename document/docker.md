# Docker

SoT: `docker-compose.yml`, `docker-compose.local.yml`, `Dockerfile`, `deploy/prometheus/prometheus.yml`.  
Quick start: [guide_setup.md](guide_setup.md). Resource: [resource_model.md](resource_model.md). Metrics: [huong_dan_prometheus_report.md](huong_dan_prometheus_report.md).

---

## Hai Compose

| File | Services | Limits | Clients |
|------|----------|--------|---------|
| `docker-compose.yml` | http2gw, **logic** (1), prometheus | Không | Không |
| `docker-compose.local.yml` | master, http2gw, **logic-1/2/3**, prometheus, **client-1…6** | Có | Profile **`load`** — chỉ khi `start.ps1` / `compose --profile load` |

Image targets: `http2gw`, `logic`, `master`, `client` (distroless nonroot).

---

## A. `docker-compose.yml` (tối thiểu)

- Config bake: `configs/http2gw.docker.yaml`, `configs/logic.docker.yaml` (`metrics.port: 9190`)
- Network: `logic-gateway-app`
- Ports host: **8080** (GW), **9090** (Prometheus), **9190** (logic metrics)

```powershell
docker compose up --build -d
docker compose logs -f
docker compose down
```

```powershell
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -body hello
curl.exe -s http://127.0.0.1:8080/metrics | findstr http2gw_
curl.exe -s http://127.0.0.1:9190/metrics | findstr logic_requests
```

Prometheus jobs: `http2gw`, `logic-single` (+ `http2gw-host` nếu scrape host).

---

## B. `docker-compose.local.yml`

### Resource (cgroup)

| Service | cpus | cpuset | mem_limit | GOMAXPROCS | Ports host |
|---------|------|--------|-----------|------------|------------|
| master | 1.0 | `"0"` | 512m | 1 | 9200 |
| http2gw | **1.0** | `"20"` | **2g** | **1** | 8080, 6060 |
| logic-1 | 2.0 | `"3-4"` | 2g | 2 | **9191→9190** |
| logic-2 | 2.0 | `"5-6"` | 2g | 2 | **9192→9190** |
| logic-3 | 2.0 | `"7-8"` | 2g | 2 | **9193→9190** |
| client-1…6 | 2.0 (file) | — | 1g | 2 | — |
| prometheus | — | — | — | — | 9090 |

Config mount: `configs/local/http2gw.docker.yaml`, `logic-{1,2,3}.docker.yaml`.  
Master bake: `configs/local/master.docker.yaml`.  
Network: `logic-gateway-local`.

`cpuset` = CPU trong **Docker VM**.

### Chỉ server

```powershell
docker compose -f docker-compose.local.yml up -d --build
docker stats --no-stream
curl.exe -s http://127.0.0.1:9200/v1/nodes
```

Client **không** chạy với lệnh trên (cần profile `load`).

### Load trong Docker

```powershell
# configs/local/load.request.yaml
.\scripts\start.ps1
.\scripts\start.ps1 -Duration 3m -Clients 3 -ClientCPUs 2
```

`start_load.ps1`:

1. `up -d --build` master, http2gw, logic-1/2/3, prometheus  
2. Build image `client` một lần  
3. Tạo `scripts/run-logs/<stamp>_client-cpus.override.yml` (`cpus` + `GOMAXPROCS`)  
4. `compose --profile load run --rm --no-deps` song song client-1…N  
5. Aggregate summary JSON  

Volumes client: `fail-logs`, `run-logs`, identity `http2client-N.docker.yaml`.

Target trong container mặc định: `http://http2gw:8080`.

### Prometheus (local)

Jobs `logic`: `logic-1:9190`, `logic-2:9190`, `logic-3:9190`.

```promql
sum by (node) (increase(http2gw_logic_routed_total[5m]))
sum by (node) (increase(logic_requests_total[5m]))
```

### Dừng

```powershell
docker ps -q --filter "name=logic-gateway-client" | ForEach-Object { docker stop $_ }
docker compose -f docker-compose.local.yml down
docker compose -f docker-compose.local.yml down -v
```

---

## Node / UDP (local docker YAML)

| Node | node_id | UDP | metrics |
|------|---------|-----|---------|
| logic-1 | 2 | 9100 | 9190 (host 9191) |
| logic-2 | 3 | 9101 | 9190 (host 9192) |
| logic-3 | 4 | 9102 | 9190 (host 9193) |
| http2gw | 10 | 9000 | :8080/metrics |
| master | 1 | — | :9200 |

`transaction.timeout` GW docker: **5000ms**.

---

## Capacity trong binary (sau rebuild)

| Mục | Code |
|-----|------|
| H2 MaxConcurrentStreams | 1024 |
| UDP buffer (Go) | Request **32 MiB** qua `Set*Buffer` + `SO_*BUFFORCE` nếu bị clamp |
| Logic DATA pool | cpu_cores×256 (2 → 512) |
| Client image | 4 H2 conn/host |

### UDP `SO_RCVBUF` trên Docker Desktop

Compose **không** set `sysctls: net.core.rmem_max` (lỗi `unsafe procfs`).  
Docker Desktop **WSL2** thường **không có** `net.core.rmem_max` → `SetReadBuffer(32MiB)` bị clamp ~**416 KiB**.

**Local compose:** `user: "0:0"` + `cap_add: [NET_ADMIN]` trên **http2gw và logic-1/2/3** (FORCE cần root; distroless nonroot → EPERM).

Binary log: `configured_rcvbuf`, `actual_rcvbuf_before_force`, `force_error`, `actual_rcvbuf_after_force`.

**Benchmark chỉ tin `actual_rcvbuf_after_force`** (configured ≠ actual; Linux thường ×2 → ~64 MiB khi request 32 MiB).
---

## Troubleshooting

| Hiện tượng | Hướng xử lý |
|------------|-------------|
| DNS `http2gw` | Phải cùng Compose network |
| Spam REGISTER | GW UDP 9000 / `gateway.host` |
| 503 | Logic chưa ACTIVE — restart logic |
| 504 + `so_rcvbuf≈425984` | Rebuild image (FORCE); `cap_add: NET_ADMIN`; restart GW |
| 504 (TX timeout khác) | Giảm concurrency; metrics TX vs RX |
| `run --cpus` unknown | Đừng thêm vào `compose run`; dùng `start.ps1` |
| Build Hub TLS timeout | Mạng `auth.docker.io` |
| Prometheus logic DOWN | Rebuild logic; kiểm tra `metrics.port: 9190` |
| Pprof không truy cập được | Kiểm tra `http.pprof_listen` và port `6060` của Gateway |
| Client connection refused | Target phải `http://http2gw:8080` trong container; stack đã healthy |
| Clients không thấy sau `up` | Cần profile `load` / `start.ps1` |
| `sysctl net.core.rmem_max` trong compose | Không dùng — WSL2 không có key; dùng `SO_*BUFFORCE` |
