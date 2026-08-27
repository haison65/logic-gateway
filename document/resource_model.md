# document/resource_model.md — Phase 7

# Resource model (Local)

## Phân biệt hai lớp

| Lớp | Ví dụ | Có phải limit OS? |
|-----|--------|-------------------|
| **Application metadata** | `resource.cpu_cores` / `resource.memory` trong YAML | **Không** — chỉ inventory/report |
| **GOMAXPROCS** | env `GOMAXPROCS=4` | **Không** — số thread Go scheduler |
| **Container cgroup** | Compose `cpus: 4`, `mem_limit: 8g` | **Có** — Docker/Linux enforce |

Không được tuyên bố “đã bind CPU” chỉ vì set `GOMAXPROCS` hoặc ghi YAML.

## Target Local (bảng)

| Node | CPU quota | Memory limit |
|------|----------:|-------------:|
| HTTP2GW | 4 | 8G |
| Logic-1 | 6 | 8G |
| Logic-2 | 6 | 8G |
| Master | 1 | 512M |
| **Tổng server** | **17** | **~24.5G** |

Máy host mục tiêu: 16 CPU / 32 GB — **quota CPU Compose (17) có thể > 16 core vật lý**.
Docker sẽ time-slice; nếu cần khít 16: giảm Master hoặc Logic cpus trong `docker-compose.local.yml`.

RAM còn lại (~7.5G+) cho OS + client/perf trên host.

## Verify

```powershell
docker compose -f docker-compose.local.yml up -d --build
docker stats --no-stream
```

Cột `MEM USAGE / LIMIT` phải phản ánh ~8Gi / 512Mi.  
Cột `CPU %` bị chặn bởi quota (không phải chứng minh affinity pin core).

## HA wording

Single-host Active-Active Logic = **application failover** (Phase 6 retry + DEAD filter).  
**Không** phải infrastructure HA (host chết → tất cả chết).
