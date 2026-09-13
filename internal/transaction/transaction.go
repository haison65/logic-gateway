package transaction

import (
	"time"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

// Config cấu hình timeout của một request/response. Độc lập heartbeat timeout.
type Config struct {
	Timeout time.Duration
	// Metrics optional instrumentation (nil = bỏ qua).
	Metrics MetricsSink
}

// RoutedCounter nhận thông báo khi gateway đã Route + Send thành công tới một Logic.
// Giữ alias tương thích; MetricsSink bao gồm method này.
type RoutedCounter = MetricsSink

func (c Config) timeoutOrDefault() time.Duration {
	if c.Timeout <= 0 {
		return DefaultTimeout
	}
	return c.Timeout
}

// Transaction là bản ghi công khai tối thiểu (design §10).
// Channel phản hồi thật nằm trong Manager (result{env,err}) để Complete-before-Wait
// và timeout/cancel không panic khi send vào chan Envelope thuần.
type Transaction struct {
	ID       uint64
	Created  time.Time
	Timeout  time.Duration
	Response chan *pb.Envelope
}
