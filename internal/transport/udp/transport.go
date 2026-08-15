// Package udp cung cấp lớp vận chuyển UDP cho thông điệp protobuf Envelope.
//
// Gói này chỉ chuyển byte giữa các node, không diễn giải REGISTER, HEARTBEAT, DATA,
// định tuyến hay ngữ nghĩa giao dịch.
package udp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

// DefaultMaxPacketSize là kích thước payload UDP tối đa trên IPv4
// (65535 - 8 byte UDP header - 20 byte IP header).
// Giao thức chưa quy định giới hạn nhỏ hơn ở tầng ứng dụng; caller có thể
// giảm qua Config.MaxPacketSize. Phân mảnh/ghép mảnh không thuộc Phase 3.
const DefaultMaxPacketSize = 65507

// DefaultReadPollInterval là chu kỳ Receive đang chặn kiểm tra ctx.Done().
const DefaultReadPollInterval = 50 * time.Millisecond

// Transport chuyển Envelope trên mạng datagram.
// Tầng ứng dụng phụ thuộc interface này, không phụ thuộc *net.UDPConn.
type Transport interface {
	Send(ctx context.Context, msg *pb.Envelope, addr *net.UDPAddr) error
	Receive(ctx context.Context) (*pb.Envelope, *net.UDPAddr, error)
	Close() error
	LocalAddr() net.Addr
}

// Config cấu hình socket UDP lắng nghe.
type Config struct {
	// ListenHost là địa chỉ bind (ví dụ "127.0.0.1" hoặc "0.0.0.0").
	ListenHost string
	// ListenPort là cổng UDP. 0 để hệ điều hành cấp cổng tạm.
	ListenPort int
	// MaxPacketSize giới hạn kích thước datagram sau khi mã hóa. 0 dùng DefaultMaxPacketSize.
	MaxPacketSize int
	// ReadPollInterval dùng khi chờ datagram để Receive nhận biết hủy context.
	// 0 dùng DefaultReadPollInterval.
	ReadPollInterval time.Duration
}

// Conn là triển khai UDP cụ thể.
//
// Quyền sở hữu: Conn nắm *net.UDPConn trong suốt vòng đời.
// Gọi Close khi xong; Close là idempotent.
//
// Đồng thời:
//   - Send an toàn khi nhiều goroutine gọi cùng lúc.
//   - Receive dành cho một goroutine tiêu thụ.
//   - Close có thể gọi đồng thời với Send/Receive.
type Conn struct {
	conn             *net.UDPConn
	maxPacketSize    int
	readPollInterval time.Duration

	closeOnce sync.Once
	closed    chan struct{}
}

// New lắng nghe trên địa chỉ UDP đã cấu hình và trả về Conn.
func New(cfg Config) (*Conn, error) {
	host := cfg.ListenHost
	if host == "" {
		host = "0.0.0.0"
	}

	addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(host, strconv.Itoa(cfg.ListenPort)))
	if err != nil {
		return nil, fmt.Errorf("resolve UDP listen address: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen UDP: %w", err)
	}

	maxSize := cfg.MaxPacketSize
	if maxSize <= 0 {
		maxSize = DefaultMaxPacketSize
	}

	poll := cfg.ReadPollInterval
	if poll <= 0 {
		poll = DefaultReadPollInterval
	}

	return &Conn{
		conn:             conn,
		maxPacketSize:    maxSize,
		readPollInterval: poll,
		closed:           make(chan struct{}),
	}, nil
}

// Đảm bảo Conn triển khai Transport.
var _ Transport = (*Conn)(nil)

// LocalAddr trả về địa chỉ UDP local mà socket đang bind.
func (c *Conn) LocalAddr() net.Addr {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.LocalAddr()
}

// Send mã hóa msg và ghi đúng một datagram UDP tới addr.
// Không tự retry và không diễn giải các trường Envelope.
func (c *Conn) Send(ctx context.Context, msg *pb.Envelope, addr *net.UDPAddr) error {
	if c == nil {
		return ErrTransportClosed
	}
	if err := c.checkOpen(ctx); err != nil {
		return err
	}
	if msg == nil {
		return fmt.Errorf("%w: nil envelope", ErrInvalidMessage)
	}
	if addr == nil {
		return fmt.Errorf("send UDP packet: nil destination address")
	}

	payload, err := encode(msg)
	if err != nil {
		return err
	}
	if len(payload) > c.maxPacketSize {
		return fmt.Errorf("%w: encoded size %d exceeds max %d", ErrPacketTooLarge, len(payload), c.maxPacketSize)
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetWriteDeadline(deadline)
		defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()
	}

	n, err := c.conn.WriteToUDP(payload, addr)
	if err != nil {
		if c.isClosed() {
			return ErrTransportClosed
		}
		if isTimeout(err) && ctx.Err() != nil {
			return fmt.Errorf("send UDP packet: %w", ctx.Err())
		}
		return fmt.Errorf("send UDP packet: %w", err)
	}
	if n != len(payload) {
		return fmt.Errorf("send UDP packet: short write %d/%d", n, len(payload))
	}
	return nil
}

// Receive đọc một datagram UDP, giải mã thành Envelope, và trả về địa chỉ người gửi.
// Gói lỗi định dạng trả về ErrInvalidMessage mà không đóng transport.
// Hủy ctx hoặc Close Conn để gỡ chặn Receive đang chờ.
func (c *Conn) Receive(ctx context.Context) (*pb.Envelope, *net.UDPAddr, error) {
	if c == nil {
		return nil, nil, ErrTransportClosed
	}

	buf := make([]byte, c.maxPacketSize)

	for {
		if err := c.checkOpen(ctx); err != nil {
			return nil, nil, err
		}

		deadline := time.Now().Add(c.readPollInterval)
		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}
		if err := c.conn.SetReadDeadline(deadline); err != nil {
			if c.isClosed() {
				return nil, nil, ErrTransportClosed
			}
			return nil, nil, fmt.Errorf("set UDP read deadline: %w", err)
		}

		n, addr, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			if c.isClosed() {
				return nil, nil, ErrTransportClosed
			}
			if isTimeout(err) {
				if ctx.Err() != nil {
					return nil, nil, fmt.Errorf("receive UDP packet: %w", ctx.Err())
				}
				continue
			}
			return nil, nil, fmt.Errorf("receive UDP packet: %w", err)
		}

		msg, err := decode(buf[:n])
		if err != nil {
			return nil, addr, err
		}
		return msg, addr, nil
	}
}

// Close đóng socket UDP và gỡ chặn các lời gọi Receive đang chờ.
// Close là idempotent và an toàn khi gọi đồng thời.
func (c *Conn) Close() error {
	if c == nil {
		return nil
	}

	var closeErr error
	c.closeOnce.Do(func() {
		close(c.closed)
		if c.conn != nil {
			closeErr = c.conn.Close()
		}
	})
	if closeErr != nil {
		return fmt.Errorf("close UDP transport: %w", closeErr)
	}
	return nil
}

func (c *Conn) checkOpen(ctx context.Context) error {
	if ctx.Err() != nil {
		return fmt.Errorf("udp transport: %w", ctx.Err())
	}
	if c.isClosed() {
		return ErrTransportClosed
	}
	return nil
}

func (c *Conn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
