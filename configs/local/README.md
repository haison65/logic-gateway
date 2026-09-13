# Local deployment configs

ID scheme (config-driven):

```text
1           = master
2, 3, 4     = logic-1, logic-2, logic-3
10          = http2gw
20..25      = http2client-1 .. http2client-6 (Docker)
20..22      = http2client-1 .. http2client-3 (dev host)
30..33      = perf-1 .. perf-4 (dev host)
```

Ports:

```text
Master HTTP          9200
HTTP2GW TCP/UDP      8080 / 9000
Logic-1/2/3 UDP      9100 / 9101 / 9102
Logic metrics        9190 in container (host 9191/9192/9193 on docker-compose.local)
Prometheus           9090
```

Files:

| File | Role |
|------|------|
| `master.dev.yaml` / `master.docker.yaml` | Master |
| `http2gw.dev.yaml` / `http2gw.docker.yaml` | Gateway (`transaction.timeout: 5000ms`) |
| `logic-{1,2,3}.dev.yaml` / `.docker.yaml` | Logic (docker có `metrics.port: 9190`) |
| `http2client-*.yaml` | Client identity |
| `perf-*.dev.yaml` | Performance identity (host load_local_p0) |
| `load.request.yaml` | Tham số Docker load cho `.\scripts\start.ps1` |

`resource.cpu_cores` / `resource.memory` = **metadata only**.  
Limit container: `docker-compose.local.yml`.

DATA-plane UDP REGISTER: HTTP2GW ↔ Logic.  
MASTER / HTTP2_CLIENT / PERFORMANCE: control-plane Master (`master_url`).
