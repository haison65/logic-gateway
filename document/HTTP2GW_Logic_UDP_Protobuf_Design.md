# HTTP2GW - Logic UDP Protobuf Communication Design

> **Phân loại tài liệu:** ý đồ thiết kế (design intent).  
> **Triển khai hiện tại (SoT = code):** xem `README.md`, `document/guide_setup.md`, `document/docker.md`.  
> Tài liệu này mô tả protocol và design intent; khi khác code, code có hiệu lực.  
>
> ### Roadmap / phase (tránh hiểu nhầm “design đã xong 100%”)
>
> | Phase | Nội dung | Trạng thái code |
> |-------|----------|-----------------|
> | **P0 (hiện tại)** | 1×HTTP2GW + N×Logic; REGISTER/HB/DATA UDP+protobuf; registry+router+transaction; Logic **echo** 1001–1003; metrics GW+Logic | **Đã có** |
> | **P1 (sau)** | `http.remote` outbound §12 (Logic→GW→HTTP remote) bật trên môi trường thật; ERROR envelope → Complete waiter (đã có trong code) | Outbound **code+test sẵn**, config local thường `remote: ""` |
> | **P2 (sau)** | **Multi-HTTP2GW cluster** (registry/route liên GW) | **Chưa** — claim “Multi HTTP2GW” ở §1 là tầm nhìn, chưa SoT |
> | **P3 (sau)** | Logic **business** thật (thay echo); auth; registry bền vững; UDP fragmentation | **Chưa** |
>
> Runtime invariant: Master là control-plane tách DATA; Gateway có một UDP receive consumer; failover ứng dụng tối đa 1 lần; `failover_retry_total` tăng trên Send failure / transaction timeout.

## 1. Overview

Thiết kế giao tiếp giữa HTTP2GW và Logic sử dụng:

-   UDP transport
-   Protocol Buffer serialization
-   Register / Heartbeat / Data message model

Mục tiêu:

-   High performance
-   Low latency
-   Dynamic registration
-   Health monitoring
-   Message routing
-   Multi Logic instance (**P0**)
-   Multi HTTP2GW instance (**P2 — chưa implement**)

------------------------------------------------------------------------

# 2. Architecture

    External Client
          |
          | HTTP/2
          |
          v

    +----------------+
    |    HTTP2GW     |
    |                |
    | HTTP2 Server   |
    | UDP Manager    |
    | Registry       |
    | Router         |
    | Transaction    |
    +-------+--------+
            |
            |
       UDP + Protobuf
            |
            |
    +-------+--------+
    |                |
    v                v

    Logic-1        Logic-2

    REGISTER
    HEARTBEAT
    DATA

------------------------------------------------------------------------

# 3. Message Category

UDP communication gồm 3 nhóm message:

  Message     Purpose
  ----------- -------------------------------------------
  REGISTER    Node discovery và capability registration
  HEARTBEAT   Kiểm tra trạng thái node
  DATA        Message nghiệp vụ

------------------------------------------------------------------------

# 4. Protobuf Envelope

``` proto
message Envelope {

    uint32 version = 1;

    MessageType type = 2;

    uint32 source_node_id = 3;

    uint32 destination_node_id = 4;

    uint64 transaction_id = 5;

    uint64 timestamp_ms = 6;

    string trace_id = 7;

    oneof body {

        RegisterRequest register_request = 20;

        RegisterResponse register_response = 21;

        HeartbeatRequest heartbeat_request = 30;

        HeartbeatResponse heartbeat_response = 31;

        DataRequest data_request = 40;

        DataResponse data_response = 41;

        ErrorMessage error = 50;

    }
}
```

------------------------------------------------------------------------

# 5. Message Type

``` proto
enum MessageType {

    UNKNOWN = 0;

    REGISTER_REQUEST = 1;

    REGISTER_RESPONSE = 2;

    HEARTBEAT_REQUEST = 3;

    HEARTBEAT_RESPONSE = 4;

    DATA_REQUEST = 10;

    DATA_RESPONSE = 11;

    ERROR = 100;
}
```

------------------------------------------------------------------------

# 6. Register Design

## Purpose

Khi HTTP2GW hoặc Logic start:

-   gửi REGISTER
-   khai báo node identity
-   khai báo capability
-   khai báo message type hỗ trợ

------------------------------------------------------------------------

## Register Request

``` proto
message RegisterRequest {

    uint32 node_id = 1;

    NodeType node_type = 2;

    string instance_id = 3;

    string ip = 4;

    uint32 port = 5;

    repeated ServiceCapability services = 10;
}
```

------------------------------------------------------------------------

## Service Capability

Logic đăng ký loại message có thể xử lý:

``` proto
message ServiceCapability {

    uint32 service_id = 1;

    string service_name = 2;

    repeated uint32 message_types = 3;
}
```

Ví dụ:

    Logic Call

    service_id = 100

    support:

    1001 CREATE_CALL
    1002 UPDATE_CALL
    1003 DELETE_CALL

------------------------------------------------------------------------

# 7. Routing

HTTP2GW duy trì routing table:

    message_type
           |
           |
           v

    1001 ---- Logic-1

    1002 ---- Logic-1

    2001 ---- Logic-2

Routing theo:

-   message_type
-   session_id
-   consistent hash

------------------------------------------------------------------------

# 8. Heartbeat

## Purpose

Kiểm tra:

-   node còn sống
-   latency
-   overload

------------------------------------------------------------------------

## Heartbeat Request

``` proto
message HeartbeatRequest {

    uint32 node_id = 1;

    uint64 sequence = 2;

    uint64 timestamp_ms = 3;

    uint32 load = 4;

    uint32 active_transaction = 5;
}
```

------------------------------------------------------------------------

## Heartbeat Response

``` proto
message HeartbeatResponse {

    uint64 sequence = 1;

    uint64 timestamp_ms = 2;

}
```

------------------------------------------------------------------------

## State Machine

    INIT

     |

    REGISTERED

     |

    ACTIVE

     |

    SUSPECT

     |

    DEAD

------------------------------------------------------------------------

# 9. Data Message

## Request

``` proto
message DataRequest {

    uint32 message_id = 1;

    string session_id = 2;

    bytes payload = 3;
}
```

------------------------------------------------------------------------

## Response

``` proto
message DataResponse {

    uint32 message_id = 1;

    uint32 status = 2;

    bytes payload = 3;

}
```

------------------------------------------------------------------------

# 10. Transaction Management

HTTP2GW quản lý:

    transaction_id
            |
            |
            +---- HTTP request
            |
            +---- UDP response

Data structure:

``` go
type Transaction struct {

    ID uint64

    Created time.Time

    Response chan Envelope

    Timeout time.Duration
}
```

------------------------------------------------------------------------

# 11. HTTP2GW Flow

    HTTP Client

        |
        |
     HTTP/2 Request

        |
        v

    HTTP2GW

        |
        | generate transaction_id

        |
        | route message

        v

    UDP DATA_REQUEST

        |

    Logic

        |

    UDP DATA_RESPONSE

        |

    HTTP Response

------------------------------------------------------------------------

# 12. Logic Outbound HTTP Flow

    Logic

       |
       |
    DATA_REQUEST

       |

    HTTP2GW

       |

    HTTP/2 Client

       |

    Remote Server

       |

    HTTP Response

       |

    DATA_RESPONSE

       |

    Logic

------------------------------------------------------------------------

# 13. UDP Processing

## Encode

``` go
data, err := proto.Marshal(msg)

conn.WriteToUDP(data, addr)
```

## Decode

``` go
var msg pb.Envelope

proto.Unmarshal(data, &msg)
```

------------------------------------------------------------------------

# 14. Registry Model

``` go
type LogicNode struct {

    NodeID uint32

    InstanceID string

    Addr *net.UDPAddr

    Services []ServiceCapability

    LastHeartbeat time.Time

    State NodeState
}
```

------------------------------------------------------------------------

# 15. Error Handling

Các lỗi:

-   UDP send failed
-   Logic timeout
-   Heartbeat timeout
-   Invalid message
-   No routing target

Mapping:

    ERROR_RESPONSE

    code:

    TIMEOUT

    NODE_NOT_AVAILABLE

    INVALID_MESSAGE

    OVERLOAD

------------------------------------------------------------------------

# 16. Configuration

``` yaml
udp:

  listen: 0.0.0.0

  port: 9000


heartbeat:

  interval: 1000ms

  timeout: 3000ms


transaction:

  timeout: 5000ms


routing:

  strategy: consistent_hash
```

------------------------------------------------------------------------

# 17. Metrics

### Design intent vs implementation

HTTP2GW exposition: `GET /metrics` (Prometheus) và `GET /metrics.json`.  
Prefix triển khai: `http2gw_`.

    http2gw_http_requests_total{status,result}
    http2gw_http_requests_fail_total{status,reason}
    http2gw_http_requests_in_flight
    http2gw_http_request_duration_seconds

    http2gw_logic_registered_total
    http2gw_heartbeat_success_total
    http2gw_heartbeat_timeout_total
    http2gw_udp_rx_total
    http2gw_udp_tx_total
    http2gw_route_failed_total
    http2gw_transaction_timeout_total
    http2gw_logic_nodes{state}
    http2gw_failover_retry_total
    http2gw_logic_routed_total{node_id,node}

Logic (khi YAML `metrics.port` > 0): `GET /metrics`, prefix `logic_`:

    logic_requests_total{node,node_id}
    logic_register_status{node,node_id}          # 1 = REGISTER accepted
    logic_heartbeat_rtt_seconds{node,node_id}    # RTT HB gần nhất
    logic_queue_size{node,node_id}               # in-flight DATA
    logic_processing_duration_seconds{...}
    (+ udp_* / pool_* chẩn đoán)

Triển khai local/Docker bổ sung (không thay mô hình message):

- UDP `SO_RCVBUF`/`SO_SNDBUF` 32 MiB request; actual OS value may be lower
- Logic DATA worker pool (`cpu_cores×256`, clamp 128–2048)
- HTTP/2 `MaxConcurrentStreams=1024`
- Client tải: 4 HTTP/2 connection / host
- Envelope `MESSAGE_TYPE_ERROR` → Manager.Fail / HTTP map (tránh 504 giả)

Vận hành / PromQL: `document/huong_dan_prometheus_report.md`.

------------------------------------------------------------------------

# 18. Final Design Summary

HTTP2GW:

-   HTTP/2 termination
-   protobuf encode/decode
-   UDP communication
-   registry management
-   routing
-   transaction correlation

Logic:

-   capability registration
-   heartbeat response
-   data handling
-   business processing (**P3** — hiện tại echo 1001–1003 cho load-test mesh)
-   outbound qua GW `http.remote` (**P1** — code sẵn, config thường tắt)

Protocol:

    REGISTER
    HEARTBEAT
    DATA

            |
            |
         UDP + ProtoBuf
