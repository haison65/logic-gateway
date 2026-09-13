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

// DefaultSocketBufferSize là SO_RCVBUF/SO_SNDBUF mặc định khi caller bật buffer (F3).
// 32 MiB: giảm Case B drop RESP dưới tải 4KB @ 1 CPU (kernel queue).
const DefaultSocketBufferSize = 32 << 20 // 32 MiB

// DefaultReceiveBatchSize là số datagram tối đa mỗi ReadBatch / drain (recvmmsg).
const DefaultReceiveBatchSize = 64

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
	// ReadBufferSize SO_RCVBUF (byte). 0 = giữ mặc định OS.
	ReadBufferSize int
	// WriteBufferSize SO_SNDBUF (byte). 0 = giữ mặc định OS.
	WriteBufferSize int
	// SendBatchSize: >0 bật gom Send đồng thời (P1.1). 0 = Send sync từng packet.
	SendBatchSize int
	// SendBatchWait: cửa sổ gom batch. 0 = 100µs.
	SendBatchWait time.Duration
	// ReceiveBatchSize: >0 bật ReceiveRawBatch (Linux recvmmsg). 0 = DefaultReceiveBatchSize khi gọi batch.
	ReceiveBatchSize int
}

// Conn là triển khai UDP cụ thể.
//
// Quyền sở hữu: Conn nắm *net.UDPConn trong suốt vòng đời.
// Gọi Close khi xong; Close là idempotent.
//
// Đồng thời:
//   - Send an toàn khi nhiều goroutine gọi cùng lúc.
//   - Receive / ReceiveRaw / ReceiveRawBatch dành cho một goroutine tiêu thụ.
//   - Close có thể gọi đồng thời với Send/Receive.
type Conn struct {
	conn             *net.UDPConn
	maxPacketSize    int
	readPollInterval time.Duration
	receiveBatchSize int
	// readBuf tái sử dụng trên receive goroutine duy nhất (không share với Send).
	readBuf []byte
	// readBatchBufs scratch cho ReceiveRawBatch (cùng receive goroutine).
	readBatchBufs [][]byte
	bufReport     BufferApplyReport

	batcher *sendBatcher

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
	if cfg.ReadBufferSize > 0 {
		if err := conn.SetReadBuffer(cfg.ReadBufferSize); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("set UDP read buffer: %w", err)
		}
	}
	if cfg.WriteBufferSize > 0 {
		if err := conn.SetWriteBuffer(cfg.WriteBufferSize); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("set UDP write buffer: %w", err)
		}
	}

	maxSize := cfg.MaxPacketSize
	if maxSize <= 0 {
		maxSize = DefaultMaxPacketSize
	}

	poll := cfg.ReadPollInterval
	if poll <= 0 {
		poll = DefaultReadPollInterval
	}
	recvBatch := cfg.ReceiveBatchSize
	if recvBatch <= 0 {
		recvBatch = DefaultReceiveBatchSize
	}

	c := &Conn{
		conn:             conn,
		maxPacketSize:    maxSize,
		readPollInterval: poll,
		receiveBatchSize: recvBatch,
		readBuf:          make([]byte, maxSize),
		closed:           make(chan struct{}),
	}
	c.bufReport = c.applySocketBuffers(cfg.ReadBufferSize, cfg.WriteBufferSize)
	if cfg.SendBatchSize > 0 {
		c.batcher = startSendBatcher(c, cfg.SendBatchSize, cfg.SendBatchWait)
	}
	return c, nil
}

// BufferApplyReport mô tả configured vs actual SO_RCVBUF/SNDBUF (benchmark phải nhìn actual).
type BufferApplyReport struct {
	ConfiguredRcvbuf       int
	RequestedRcvbuf        int
	ActualRcvbufBeforeForce int
	ForceError             error
	ActualRcvbufAfterForce int
	ConfiguredSndbuf       int
	RequestedSndbuf        int
	ActualSndbufBeforeForce int
	ActualSndbufAfterForce int
	Forced                 bool
}

// BufferApplyReport trả về kết quả apply buffer lần New (zero nếu không set buffer).
func (c *Conn) BufferApplyReport() BufferApplyReport {
	if c == nil {
		return BufferApplyReport{}
	}
	return c.bufReport
}

// applySocketBuffers: Set*Buffer rồi FORCE nếu bị clamp. Không nuốt force_error (ghi vào report).
func (c *Conn) applySocketBuffers(wantR, wantW int) BufferApplyReport {
	rep := BufferApplyReport{
		ConfiguredRcvbuf: wantR,
		RequestedRcvbuf:  wantR,
		ConfiguredSndbuf: wantW,
		RequestedSndbuf:  wantW,
	}
	if c == nil || (wantR <= 0 && wantW <= 0) {
		return rep
	}
	beforeR, beforeW, err := c.SocketBufferSizes()
	if err != nil {
		rep.ForceError = fmt.Errorf("getsockopt before force: %w", err)
		// Vẫn thử FORCE.
		if ferr := c.forceSocketBuffers(wantR, wantW); ferr != nil {
			rep.ForceError = fmt.Errorf("%v; force: %w", rep.ForceError, ferr)
		} else {
			rep.Forced = true
		}
	} else {
		rep.ActualRcvbufBeforeForce = beforeR
		rep.ActualSndbufBeforeForce = beforeW
		needForce := (wantR > 0 && beforeR > 0 && beforeR < wantR) ||
			(wantW > 0 && beforeW > 0 && beforeW < wantW) ||
			(wantR > 0 && beforeR == 0) || (wantW > 0 && beforeW == 0)
		if needForce {
			rep.Forced = true
			if ferr := c.forceSocketBuffers(wantR, wantW); ferr != nil {
				rep.ForceError = ferr
			}
		}
	}
	afterR, afterW, aerr := c.SocketBufferSizes()
	if aerr != nil {
		if rep.ForceError == nil {
			rep.ForceError = fmt.Errorf("getsockopt after force: %w", aerr)
		} else {
			rep.ForceError = fmt.Errorf("%v; after: %w", rep.ForceError, aerr)
		}
		return rep
	}
	rep.ActualRcvbufAfterForce = afterR
	rep.ActualSndbufAfterForce = afterW
	return rep
}

// Đảm bảo Conn triển khai RawReceiver / RawBatchReceiver (decode off receive path).
var _ RawReceiver = (*Conn)(nil)
var _ RawBatchReceiver = (*Conn)(nil)

// Đảm bảo Conn triển khai Transport.
var _ Transport = (*Conn)(nil)

// ReceiveBatchSize trả về kích thước batch nhận đã cấu hình.
func (c *Conn) ReceiveBatchSize() int {
	if c == nil || c.receiveBatchSize <= 0 {
		return DefaultReceiveBatchSize
	}
	return c.receiveBatchSize
}

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

	payload, release, err := encode(msg)
	if err != nil {
		return err
	}
	if len(payload) > c.maxPacketSize {
		release()
		return fmt.Errorf("%w: encoded size %d exceeds max %d", ErrPacketTooLarge, len(payload), c.maxPacketSize)
	}

	if c.batcher != nil {
		return c.batcher.submit(ctx, payload, release, addr)
	}
	defer release()

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

// ReceiveRaw đọc một datagram và trả về bản copy owned (pool). Caller phải ReleasePacket.
// Không protobuf-decode — dùng cho dispatch receive loop (Case B).
func (c *Conn) ReceiveRaw(ctx context.Context) ([]byte, *net.UDPAddr, error) {
	if c == nil {
		return nil, nil, ErrTransportClosed
	}

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

		n, addr, err := c.conn.ReadFromUDP(c.readBuf)
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

		pkt := acquirePacket(n)
		copy(pkt, c.readBuf[:n])
		return pkt, addr, nil
	}
}

// Receive đọc một datagram UDP, giải mã thành Envelope, và trả về địa chỉ người gửi.
// Gói lỗi định dạng trả về ErrInvalidMessage mà không đóng transport.
// Hủy ctx hoặc Close Conn để gỡ chặn Receive đang chờ.
func (c *Conn) Receive(ctx context.Context) (*pb.Envelope, *net.UDPAddr, error) {
	pkt, addr, err := c.ReceiveRaw(ctx)
	if err != nil {
		return nil, addr, err
	}
	defer ReleasePacket(pkt)
	msg, err := decode(pkt)
	if err != nil {
		return nil, addr, err
	}
	return msg, addr, nil
}

// Close đóng socket UDP và gỡ chặn các lời gọi Receive đang chờ.
// Close là idempotent và an toàn khi gọi đồng thời.
func (c *Conn) Close() error {
	if c == nil {
		return nil
	}

	var closeErr error
	c.closeOnce.Do(func() {
		if c.batcher != nil {
			c.batcher.close()
		}
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
