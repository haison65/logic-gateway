# Resource model

## Ba lớp — không lẫn

| Lớp | Ví dụ | Enforce OS? |
|-----|--------|-------------|
| YAML `resource.cpu_cores` / `memory` | inventory Master / báo cáo | **Không** |
| `GOMAXPROCS` | số thread scheduler Go | **Không** (cgroup vẫn siết) |
| Compose `cpus` / `mem_limit` / `cpuset` | Docker cgroup | **Có** |

---

## `docker-compose.local.yml` (số trong file)

| Service | cpus | cpuset | mem_limit | GOMAXPROCS |
|---------|------|--------|-----------|------------|
| master | 1.0 | `"0"` | 512m | 1 |
| http2gw | 1.0 | `"20"` | 2g | 1 |
| logic-1 | 2.0 | `"3-4"` | 2g | 2 |
| logic-2 | 2.0 | `"5-6"` | 2g | 2 |
| logic-3 | 2.0 | `"7-8"` | 2g | 2 |
| **Σ server** | **8** | | **~8.5G** | |
| client-* (load) | 2.0 mặc định (file); ghi đè bằng `start.ps1 -ClientCPUs` | không pin | 1g | ceil(cpus) |

`cpuset` gắn CPU **trong Docker VM**. Host Windows không map 1:1.

`docker-compose.yml` (tối thiểu): **không** set cpus/mem.

---

## Client CPU override

`start_load.ps1` tạo `scripts/run-logs/<stamp>_client-cpus.override.yml` rồi:

```text
docker compose -f docker-compose.local.yml -f <override> --profile load run …
```

**Không** dùng `docker compose run --cpus` — Compose v5 báo `unknown flag`.

---

## Capacity liên quan code

| Mục | Giá trị |
|-----|---------|
| Logic DATA workers | `max(128, min(2048, cpu_cores×256))`; thiếu `cpu_cores` → 512 |
| UDP buffer | 32 MiB request; actual OS value may be lower |
| H2 streams/conn | 1024 |

YAML `logic-*.dev.yaml` có thể ghi `cpu_cores: 6` — đó là **metadata**, không phải limit `start_local`.

---

## Verify

```powershell
docker compose -f docker-compose.local.yml up -d --build
docker stats --no-stream
```

---

## HA

Single-host Active-Active = failover ứng dụng. Host chết → toàn stack chết. Registry không persistent.
