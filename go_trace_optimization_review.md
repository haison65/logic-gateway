# Phân tích Trace và giải pháp tối ưu Go Server

## 1. Tổng quan phân tích

Kết quả phân tích Go runtime trace cho thấy hệ thống hiện tại không bị
CPU compute bottleneck thuần túy.

Bottleneck chính:

1.  HTTP/2 request processing.
2.  Memory allocation và GC do xử lý request body.
3.  Channel synchronization giữa request và UDP response.
4.  UDP syscall overhead.
5.  Goroutine scheduling/concurrency model.

Kết luận:

> Hệ thống hiện tại thiên về I/O-bound và synchronization-bound thay vì
> CPU-bound.

------------------------------------------------------------------------

# 2. HTTP/2 request handling

## Hiện trạng

Hot path:

    http2.(*serverConn).serve
    http2.requestBody.Read
    io.ReadAll

Chiếm khoảng 42% sched time.

Flow hiện tại:

    HTTP/2 DATA FRAME
            |
            v
    io.ReadAll(req.Body)
            |
            v
    allocate []byte
            |
            v
    JSON parse
            |
            v
    process

## Vấn đề

-   Mỗi request tạo buffer mới.
-   Copy dữ liệu.
-   Tăng allocation.
-   Tăng GC pressure.

------------------------------------------------------------------------

## Giải pháp 2.1: Không dùng io.ReadAll khi không cần

Trước:

``` go
body, _ := io.ReadAll(req.Body)

json.Unmarshal(body, &request)
```

Sau:

``` go
decoder := json.NewDecoder(req.Body)

decoder.Decode(&request)
```

Lợi ích:

  Metric        Cải thiện
  ------------- -----------
  Allocation    Giảm
  GC            Giảm
  Memory peak   Giảm
  Latency       Giảm

------------------------------------------------------------------------

# 3. Sử dụng sync.Pool

## Mục đích

`sync.Pool` dùng để tái sử dụng object tạm thời, giảm allocation và giảm
áp lực GC.

Phù hợp với:

-   \[\]byte buffer.
-   RTP packet.
-   UDP packet.
-   Transaction context.

Không dùng cho:

-   Connection.
-   Config.
-   Object sống lâu.

------------------------------------------------------------------------

## Ví dụ buffer pool

``` go
var bufferPool = sync.Pool{
    New: func() any {
        return make([]byte, 8192)
    },
}
```

Flow:

    Request
       |
     Get buffer
       |
     Process
       |
     Reset
       |
     Put buffer

------------------------------------------------------------------------

# 4. Tối ưu Manager.Wait / Manager.Complete

## Hiện trạng

Trace cho thấy:

    selectgo
    chanrecv
    chansend
    runtime.selectnbsend

rất lớn.

Flow khả năng:

    HTTP request

        |
        v

    Create response channel

        |
        v

    Wait response

        |
        v

    UDP response

        |
        v

    Manager.Complete()

        |
        v

    channel notify

------------------------------------------------------------------------

## Vấn đề

Mỗi transaction tạo:

``` go
respCh := make(chan Response)
```

Traffic lớn:

    5000 request/s
    =
    5000 channel object/s

Tạo nhiều allocation.

------------------------------------------------------------------------

## Giải pháp

### 4.1 Transaction pool

Ví dụ:

``` go
type Transaction struct {
    ID uint64
    Response chan Response
    Timestamp time.Time
}
```

Pool:

``` go
var txPool = sync.Pool{
    New: func() any {
        return &Transaction{
            Response: make(chan Response, 1),
        }
    },
}
```

Flow:

    Request
     |
    Get transaction
     |
    Send UDP
     |
    Complete
     |
    Reset
     |
    Put

------------------------------------------------------------------------

## 4.2 Buffered channel

Thay:

``` go
make(chan Response)
```

Bằng:

``` go
make(chan Response, 1)
```

Giảm block giữa response goroutine và waiter.

------------------------------------------------------------------------

# 5. Tối ưu UDP processing

## Hiện trạng

Syscall:

    sendto  ~28%
    recvfrom ~5%

UDP send là phần đáng chú ý.

------------------------------------------------------------------------

## Giải pháp 5.1: UDP packet pool

Thay:

``` go
packet := &Packet{}
```

Bằng:

``` go
packet := packetPool.Get()
```

Sau xử lý:

``` go
packetPool.Put(packet)
```

Áp dụng:

-   RTP packet.
-   UDP request.
-   UDP response.

------------------------------------------------------------------------

## Giải pháp 5.2: UDP batching

Hiện tại:

    packet
     |
     sendto()

Mỗi packet một syscall.

Tối ưu:

    packet queue

        |
        v

    batch send

        |
        v

    sendmmsg()

Linux hỗ trợ:

-   sendmmsg()
-   recvmmsg()

Lợi ích:

-   Giảm syscall.
-   Tăng throughput.

------------------------------------------------------------------------

# 6. Tối ưu Goroutine model

## Worker pool

Không tạo goroutine vô hạn theo request.

Hiện:

    Request
       |
    goroutine mới

Đề xuất:

    Request
       |
    Queue
       |
    Worker Pool
       |
    Process

------------------------------------------------------------------------

## Concurrency limit

Dùng semaphore:

``` go
sem := make(chan struct{}, 1000)
```

Mục tiêu:

-   bảo vệ memory.
-   tránh goroutine explosion.
-   ổn định latency.

------------------------------------------------------------------------

# 7. GC và memory optimization

Hiện tại GC chưa phải bottleneck.

Tuy nhiên sau khi tăng traffic cần tối ưu:

## Nên dùng sync.Pool cho:

  Object                Pool
  --------------------- ------
  \[\]byte buffer       Có
  RTP packet            Có
  Transaction context   Có
  JSON buffer           Có

------------------------------------------------------------------------

# 8. Bổ sung monitoring

## CPU profile

``` bash
curl localhost:6060/debug/pprof/profile?seconds=60 > cpu.pprof
```

Phân tích:

    function
    CPU %

------------------------------------------------------------------------

## Block profile

Bật:

``` go
runtime.SetBlockProfileRate(1)
```

Dùng để tìm:

-   channel block.
-   mutex block.

------------------------------------------------------------------------

## Mutex profile

Bật:

``` go
runtime.SetMutexProfileFraction(1)
```

Tìm:

-   lock contention.

------------------------------------------------------------------------

# 9. Kiến trúc tối ưu đề xuất

Hiện tại:

    HTTP/2
     |
    Handler
     |
    create channel
     |
    UDP
     |
    Complete()
     |
    channel wakeup
     |
    HTTP response

Đề xuất:

                 HTTP/2

                    |

              Request Pool

                    |

            Transaction Manager

                    |

              UDP Worker Pool

                    |

              UDP Batch Send

                    |

            Response Dispatcher

                    |

                Callback

                    |

              HTTP/2 Response

------------------------------------------------------------------------

# 10. Roadmap triển khai

  Priority   Hạng mục                       Tác động
  ---------- ------------------------------ -----------------------
  P0         Bỏ io.ReadAll                  Giảm allocation
  P0         sync.Pool transaction          Giảm GC
  P0         Review Manager.Wait/Complete   Giảm channel overhead
  P1         Buffered channel               Giảm block
  P1         UDP packet pool                Giảm memory
  P1         Worker pool                    Ổn định tải cao
  P1         sendmmsg/recvmmsg              Tăng UDP throughput
  P2         Prometheus metrics             Monitoring
  P2         CPU/block/mutex profile        Tối ưu tiếp

------------------------------------------------------------------------

# Kết luận

Ưu tiên triển khai:

1.  Review Manager.Wait/Complete.
2.  Loại bỏ io.ReadAll và giảm allocation.
3.  Áp dụng sync.Pool cho transaction, buffer, packet.
4.  Tối ưu UDP syscall bằng batching.
5.  Bổ sung pprof metrics để đo trước/sau.

Các thay đổi này tập trung giảm overhead của runtime, giảm GC và tăng
khả năng xử lý concurrent request thay vì chỉ tăng CPU.
