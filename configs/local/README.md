# Local deployment configs (Phase 2+)
#
# ID scheme (deterministic, config-driven):
#   1      = master
#   2, 3   = logic-1, logic-2
#   10     = http2gw (HTTP2_SERVER)
#   20, 21 = http2client-1, http2client-2
#   30..33 = perf-1 .. perf-4
#
# Ports:
#   Master HTTP 9200
#   HTTP2GW TCP 8080 / UDP 9000
#   Logic-1 UDP 9100 / Logic-2 UDP 9101
#
# resource.cpu_cores / resource.memory = application metadata only.
# Real OS/container limits: docker-compose.local.yml (Phase 7).
#
# DATA-plane UDP REGISTER still only HTTP2GW | LOGIC.
# MASTER / HTTP2_CLIENT / PERFORMANCE register on Master control-plane (Phase 3–4).
