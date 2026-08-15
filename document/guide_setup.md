# Hướng dẫn dựng môi trường và chạy E2E

Tài liệu này mô tả cách chạy vòng:

```text
Client (cmd/client, HTTP/2)
        │
        ▼
http2gw (HTTP :8080, UDP :9000)
        │
        ▼
Logic   (UDP :9100, echo DATA 1001/1002/1003)
        │
        ▼
http2gw ghép transaction_id
        │
        ▼
Client nhận HTTP/2 200 + body
```

Vòng **client → http2gw → Logic echo** không cần `http.remote`. Trường `http.remote` chỉ dùng khi Logic gửi `DATA_REQUEST` ra gateway để gateway gọi HTTP/2 tới server khác (xem mục 8).

Mọi lệnh chạy từ thư mục repo:

```text
d:\Golang\project\logic-gateway
```

Dùng PowerShell. Không dùng `curl`; client E2E là `cmd/client`.

------------------------------------------------------------------------

## 1. Yêu cầu

- Go trên `PATH` (`go version`)
- Source đã generate protobuf (`proto/gen/go/` đã có trong repo)
- Cổng trống:

  | Process   | Bind              | Vai trò                         |
  | --------- | ----------------- | ------------------------------- |
  | http2gw   | `127.0.0.1:8080`  | HTTP/2 (h2c) nhận client        |
  | http2gw   | `127.0.0.1:9000`  | UDP: REGISTER / HEARTBEAT / DATA |
  | logic     | `127.0.0.1:9100`  | UDP Logic, `node_id: 2`         |

Kiểm tra cổng:

```powershell
netstat -ano | findstr "8080 9000 9100"
```

Nếu cổng bị chiếm, tắt process cũ hoặc đổi port trong `configs/*.dev.yaml`.

Config dùng cho local:

- `configs/http2gw.dev.yaml`
- `configs/logic.dev.yaml`

------------------------------------------------------------------------

## 2. Test E2E tự động (không cần chạy binary)

Không chiếm cổng 8080/9000/9100. Go tự mở UDP + HTTP/2 + Logic echo.

```powershell
cd d:\Golang\project\logic-gateway
go test ./internal/logicnode -run TestHTTPClientEchoViaLogic -v
```

Kỳ vọng: `PASS`. Test kiểm tra:

- client ↔ http2gw là HTTP/2
- body được echo
- có header `X-Transaction-Id`

------------------------------------------------------------------------

## 3. E2E chạy tay với `cmd/client`

Cần **3 terminal**. Thứ tự bắt buộc: **http2gw → logic → client**.

### Terminal 1 — http2gw

```powershell
cd d:\Golang\project\logic-gateway
go run ./cmd/http2gw -config configs/http2gw.dev.yaml
```

Mặc định `-debug=true`: mỗi request in `http data in/out` (kèm `body`/`payload`), `tx create/route/send/complete`, `udp data response` (kèm `payload`). Tắt: `-debug=false`.

Log JSON (zap) gồm `time`, `level`, `module`, `line`, `Message`. Ví dụ `module` là `http2gw`, `http2gw.httpsrv`, `http2gw.dispatch`, `http2gw.transaction`.

Log cần có: đang lắng nghe UDP `127.0.0.1:9000` và HTTP `127.0.0.1:8080`.

Để terminal này chạy. Không Ctrl+C.

### Terminal 2 — logic

```powershell
cd d:\Golang\project\logic-gateway
go run ./cmd/logic -config configs/logic.dev.yaml
```

Mặc định `-debug=true`: mỗi DATA in `udp data request` và `udp data out`. Tắt: `-debug=false`.

Log JSON gồm `time`, `level`, `module` (`logic`), `line`, `Message`.

Log cần có: `logic đã đăng ký`.

Đợi khoảng **1 giây** để HEARTBEAT đưa node sang trạng thái **ACTIVE**. Router chỉ chọn node ACTIVE. Gọi client trước lúc này sẽ nhận HTTP **503**.

Để terminal này chạy.

### Terminal 3 — client Go (một request)

```powershell
cd d:\Golang\project\logic-gateway
go run ./cmd/client -addr http://127.0.0.1:8080 -message-id 1001 -session-id sess-1 -body hello
```

Kỳ vọng:

```text
proto: HTTP/2
status: 200
X-Transaction-Id: <số>
latency: ...
body: hello
```

Ý nghĩa:

- `proto: HTTP/2` — client nói HTTP/2 (h2c) với http2gw
- `X-Transaction-Id` — UDP `DATA_RESPONSE` đã được ghép
- `body: hello` — Logic echo payload (`message_id` 1001/1002/1003)

`X-Message-Id` phải là `1001`, `1002` hoặc `1003` (Logic quảng bá trong yaml). Id khác: Logic trả Envelope ERROR trên UDP, client thường nhận HTTP **502**.

------------------------------------------------------------------------

## 4. Đẩy tải bằng cùng client

http2gw và logic vẫn chạy. Terminal 3:

```powershell
go run ./cmd/client -n 100 -c 10
go run ./cmd/client -n 1000 -c 50 -unique-session
go run ./cmd/client -d 10s -c 20
go run ./cmd/client -n 5000 -c 50 -qps 200 -v
```

Client dùng **một kết nối HTTP/2**, nhiều stream song song.

Kỳ vọng: `fail: 0`, `ok` bằng tổng request, `http2` gần bằng số request.

Dừng tải: Ctrl+C ở terminal client. Dừng hệ thống: Ctrl+C logic, rồi http2gw.

### Cờ `cmd/client`

| Cờ                 | Mặc định                  | Ý nghĩa                                              |
| ------------------ | ------------------------- | ---------------------------------------------------- |
| `-addr`            | `http://127.0.0.1:8080`   | Địa chỉ http2gw                                      |
| `-message-id`      | `1001`                    | `X-Message-Id`                                       |
| `-session-id`      | `sess-1`                  | `X-Session-Id` (consistent-hash)                     |
| `-unique-session`  | `false`                   | Mỗi request một session-id (trải nhiều Logic)        |
| `-trace-id`        | tự tạo                    | `X-Trace-Id`                                         |
| `-body`            | `hello`                   | Payload gửi Logic                                    |
| `-timeout`         | `10s`                     | Timeout từng request                                 |
| `-n`               | `1`                       | Tổng số request (`0` = không giới hạn, dùng với `-d`) |
| `-c`               | `1`                       | Số goroutine / stream đồng thời                      |
| `-d`               | `0`                       | Chạy theo thời gian, ví dụ `10s`                     |
| `-qps`             | `0`                       | Trần request/giây (`0` = không giới hạn)             |
| `-v`               | `false`                   | In từng request lỗi khi đẩy tải                      |

Khi `-n 1 -c 1` (không `-d`): in chi tiết một response. Khi đẩy tải: in `ok/fail/http2/rps` và latency min/avg/p50/p95/p99/max. Có fail thì exit code 1.

------------------------------------------------------------------------

## 5. Luồng một request

```text
cmd/client  --HTTP/2 POST /v1/data-->  http2gw :8080
http2gw     --UDP DATA_REQUEST------>  logic  :9100
logic       --UDP DATA_RESPONSE----->  http2gw :9000
http2gw     --HTTP/2 200 + body----->  cmd/client
```

http2gw:

1. `POST /v1/data` → Envelope `DATA_REQUEST`
2. Transaction Manager cấp `transaction_id` **trước** khi Send
3. Router chọn Logic `ACTIVE` hỗ trợ `message_id`
4. UDP Send một datagram
5. Wait
6. Vòng Receive duy nhất: `DATA_RESPONSE` → `OnResponse` → Complete
7. Trả HTTP/2 cho client

Logic:

1. REGISTER tới `127.0.0.1:9000`
2. HEARTBEAT định kỳ → `ACTIVE`
3. Mỗi `DATA_REQUEST` với `message_id` 1001/1002/1003 echo `DATA_RESPONSE` (cùng `transaction_id` và payload). Spec chưa định nghĩa nghiệp vụ CREATE/UPDATE/DELETE — local chỉ echo.

------------------------------------------------------------------------

## 6. Lỗi thường gặp

| Hiện tượng                         | Nguyên nhân / cách xử lý                                      |
| ---------------------------------- | ------------------------------------------------------------- |
| `connection refused`               | Terminal 1 chưa chạy, hoặc sai `-addr`                        |
| `bind: address already in use`     | Cổng 8080/9000/9100 đang bị chiếm                             |
| `không đọc được cấu hình`          | Không chạy từ root repo, sai đường dẫn `-config`              |
| HTTP **503** `no eligible node`    | Logic chưa lên, chưa REGISTER, hoặc chưa HEARTBEAT (chưa ACTIVE) |
| HTTP **504**                       | Logic không echo / UDP không tới `node.ip:port`               |
| HTTP **502**                       | Envelope không phải `DATA_RESPONSE` (ví dụ `message-id` lạ), UDP lỗi, manager đóng |
| HTTP **400**                       | Thiếu hoặc sai `-message-id`                                  |
| `cảnh báo: không phải HTTP/2`      | Không dùng `cmd/client` (ví dụ HTTP/1.1 default client)       |

Windows Firewall với `127.0.0.1` thường không chặn. Nếu UDP lạ, cho phép Go với Private network.

------------------------------------------------------------------------

## 7. Health và metrics

`GET /healthz` — process còn sống:

```powershell
curl.exe -s http://127.0.0.1:8080/healthz
```

Kỳ vọng: `{"status":"ok"}`.

`GET /metrics` — JSON gồm thống kê `POST /v1/data` **và** các bộ đếm thiết kế (`logic_registered_total`, `heartbeat_*`, `udp_rx_total` / `udp_tx_total`, `route_failed_total`, `transaction_timeout_total`). Không tính `/healthz` hay `/metrics` vào `requests_*`.

```powershell
curl.exe -s http://127.0.0.1:8080/metrics
```

Ví dụ:

```json
{
  "requests_total": 1000,
  "requests_ok": 995,
  "requests_fail": 5,
  "in_flight": 0,
  "by_status": { "200": 995, "400": 1, "503": 2, "504": 2 },
  "fail_by_code": {
    "400": 1,
    "413": 0,
    "499": 0,
    "502": 0,
    "503": 2,
    "504": 2
  },
  "fail_by_reason": {
    "missing_message_id": 1,
    "no_routing_target": 2,
    "timeout": 2,
    "invalid_body": 0,
    "body_too_large": 0,
    "invalid_request": 0,
    "invalid_node": 0,
    "canceled": 0,
    "empty_response": 0,
    "udp_send_failed": 0,
    "manager_closed": 0,
    "logic_error": 0,
    "unknown": 0
  },
  "latency": {
    "count": 1000,
    "min_ms": 1.2,
    "max_ms": 48.0,
    "avg_ms": 8.5,
    "sum_ms": 8500
  },
  "logic_registered_total": 1,
  "heartbeat_success_total": 12,
  "heartbeat_timeout_total": 0,
  "udp_rx_total": 20,
  "udp_tx_total": 8,
  "route_failed_total": 2,
  "transaction_timeout_total": 2
}
```

`requests_fail` là tổng. Chi tiết:

| HTTP | `fail_by_reason` | Khi nào |
| --- | --- | --- |
| 400 | `missing_message_id` | Thiếu/sai `X-Message-Id` |
| 400 | `invalid_body` | Không đọc được body |
| 400 | `invalid_request` / `invalid_node` | Envelope/node đích không hợp lệ |
| 413 | `body_too_large` | Body > 1 MiB |
| 499 | `canceled` | Client hủy |
| 502 | `udp_send_failed` / `empty_response` / `manager_closed` / `unknown` | Gửi UDP lỗi, không có `DATA_RESPONSE`, manager đóng, lỗi chưa phân loại |
| 503 | `no_routing_target` | Không có Logic ACTIVE |
| 504 | `timeout` | Hết `transaction.timeout` |
| 4xx/5xx từ Logic | `logic_error` | `DataResponse.status` lỗi |

Các khóa trong `fail_by_code` / `fail_by_reason` luôn có mặt; giá trị `0` nếu chưa xảy ra.

`latency` đo từ lúc vào `handleData` đến lúc ghi xong HTTP (gồm Route + UDP Send + Wait). Số liệu cộng dồn từ lúc process start. Vòng DATA inbound vẫn nên dùng `cmd/client` để đảm bảo HTTP/2.

------------------------------------------------------------------------

## 8. Chiều Logic → HTTP/2 remote (không bắt buộc cho mục 3)

Design có chiều: Logic gửi `DATA_REQUEST` UDP → http2gw gọi **HTTP/2 Client** → remote → `DATA_RESPONSE` (hoặc Envelope ERROR) về Logic.

- Cấu hình: `http.remote` (base URL), ví dụ `http://127.0.0.1:9090`.
- `configs/http2gw.dev.yaml` **không** set `remote` → để trống. Logic gọi outbound sẽ nhận ERROR `NO_ROUTING_TARGET`.
- Request: `POST {remote}/v1/data`, header `X-Message-Id` / `X-Session-Id` / `X-Trace-Id` / `X-Transaction-Id`, body = payload. `http://` dùng h2c (HTTP/2 prior-knowledge), cùng kiểu client `cmd/client`.
- HTTP 4xx/5xx: vẫn `DATA_RESPONSE` với `status` = mã HTTP. Timeout / không kết nối được: Envelope ERROR trên UDP.
- URL/path không có trong design; đây là quy ước triển khai, không đổi spec.

Không cần mục này để chạy `cmd/client` echo local.
