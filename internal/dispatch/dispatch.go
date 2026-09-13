package dispatch

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/haison65/logic-gateway/internal/logger"
	"github.com/haison65/logic-gateway/internal/metrics"
	"github.com/haison65/logic-gateway/internal/outbound"
	"github.com/haison65/logic-gateway/internal/protocol"
	"github.com/haison65/logic-gateway/internal/transport/udp"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
	"go.uber.org/zap"
)

const (
	defaultInboundQueueSize = 8192
	defaultInboundWorkers   = 2
)

// Registrar xử lý REGISTER_REQUEST.
type Registrar interface {
	Register(req *pb.RegisterRequest) (*pb.RegisterResponse, error)
}

// Heartbeater xử lý HEARTBEAT_REQUEST.
type Heartbeater interface {
	Handle(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error)
}

// Completer nhận DATA_RESPONSE. Không gọi Transport.Receive.
type Completer interface {
	OnResponse(env *pb.Envelope) error
}

// Outbound chuyển DATA_REQUEST từ Logic sang HTTP remote (§12).
type Outbound interface {
	Configured() bool
	Forward(ctx context.Context, env *pb.Envelope) (*pb.DataResponse, error)
}

// Config cấu hình inbound queue giữa UDP Receive và workers.
type Config struct {
	// InboundQueueSize: bounded channel. <=0 → defaultInboundQueueSize.
	InboundQueueSize int
	// InboundWorkers: số worker cố định. <=0 → defaultInboundWorkers.
	InboundWorkers int
}

func (c Config) normalized() Config {
	if c.InboundQueueSize <= 0 {
		c.InboundQueueSize = defaultInboundQueueSize
	}
	if c.InboundWorkers <= 0 {
		c.InboundWorkers = defaultInboundWorkers
	}
	return c
}

type inboundMsg struct {
	env  *pb.Envelope
	from *net.UDPAddr
}

// Dispatcher là consumer duy nhất của Transport.Receive.
// Receive loop chỉ đọc + enqueue; workers gọi handle (Complete / heartbeat / …).
type Dispatcher struct {
	nodeID    uint32
	tr        udp.Transport
	reg       Registrar
	hb        Heartbeater
	tx        Completer
	ob        Outbound
	met       *metrics.Metrics
	log       *zap.Logger
	queueSize int
	workers   int
}

// New tạo Dispatcher. tr, reg, hb, tx không được nil. ob và met được phép nil.
func New(nodeID uint32, tr udp.Transport, reg Registrar, hb Heartbeater, tx Completer, log *zap.Logger, ob Outbound, met *metrics.Metrics, cfg Config) (*Dispatcher, error) {
	if nodeID == 0 {
		return nil, fmt.Errorf("dispatcher node_id must be > 0")
	}
	if tr == nil {
		return nil, fmt.Errorf("dispatcher transport is required")
	}
	if reg == nil {
		return nil, fmt.Errorf("dispatcher registrar is required")
	}
	if hb == nil {
		return nil, fmt.Errorf("dispatcher heartbeater is required")
	}
	if tx == nil {
		return nil, fmt.Errorf("dispatcher completer is required")
	}
	cfg = cfg.normalized()
	log = logger.OrNop(log)
	return &Dispatcher{
		nodeID:    nodeID,
		tr:        tr,
		reg:       reg,
		hb:        hb,
		tx:        tx,
		ob:        ob,
		met:       met,
		log:       log,
		queueSize: cfg.InboundQueueSize,
		workers:   cfg.InboundWorkers,
	}, nil
}

// Run đọc UDP cho đến khi ctx hủy hoặc transport đóng.
// Architecture: Receive → bounded queue → N workers → handle.
func (d *Dispatcher) Run(ctx context.Context) error {
	if d == nil {
		return fmt.Errorf("dispatcher is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	queue := make(chan inboundMsg, d.queueSize)
	d.log.Info("dispatch inbound queue",
		zap.Int("queue_size", d.queueSize),
		zap.Int("workers", d.workers),
	)

	var wg sync.WaitGroup
	for i := 0; i < d.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range queue {
				d.met.SetUDPResponseQueueDepth(len(queue))
				d.handle(ctx, msg.env, msg.from)
			}
		}()
	}

	defer func() {
		close(queue)
		wg.Wait()
		d.met.SetUDPResponseQueueDepth(0)
	}()

	for {
		env, from, err := d.tr.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, udp.ErrTransportClosed) {
				return err
			}
			if errors.Is(err, udp.ErrInvalidMessage) {
				d.met.AddUDPRx(1)
				d.met.AddGWUDPReceiveError(1)
				d.log.Warn("udp envelope không hợp lệ", zap.Error(err), zap.String("from", addrString(from)))
				d.sendError(ctx, from, nil, pb.ErrorCode_ERROR_CODE_INVALID_MESSAGE, err.Error())
				continue
			}
			d.met.AddGWUDPReceiveError(1)
			return err
		}
		d.met.AddUDPRx(1)

		msg := inboundMsg{env: env, from: from}
		select {
		case queue <- msg:
			d.met.SetUDPResponseQueueDepth(len(queue))
		default:
			// Bounded full: drop observable (không block Receive → tránh OS RCVBUF overflow).
			d.met.AddUDPResponseQueueFull(1)
			d.met.AddUDPResponseDropped(1)
			if shouldLogQueueFull(&queueFullLogNanos) {
				d.log.Warn("udp inbound queue full; dropping envelope after socket read",
					zap.String("type", envelopeType(env)),
					zap.Uint64("transaction_id", envelopeTxID(env)),
					zap.Int("queue_size", d.queueSize),
				)
			}
		}
	}
}

func (d *Dispatcher) handle(ctx context.Context, env *pb.Envelope, from *net.UDPAddr) {
	if env == nil {
		return
	}
	switch env.GetType() {
	case pb.MessageType_MESSAGE_TYPE_REGISTER_REQUEST:
		d.onRegister(ctx, env, from)
		udp.ReleaseEnvelope(env)
	case pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST:
		d.onHeartbeat(ctx, env, from)
		udp.ReleaseEnvelope(env)
	case pb.MessageType_MESSAGE_TYPE_HEARTBEAT_RESPONSE:
		// Phản hồi cho HEARTBEAT HTTP2GW → Logic; không ApplyHeartbeat (Logic đã sống qua chiều ngược).
		d.log.Debug("udp heartbeat response",
			zap.Uint64("transaction_id", env.GetTransactionId()),
			zap.String("from", addrString(from)),
			zap.Uint64("sequence", heartbeatResponseSequence(env)),
		)
		udp.ReleaseEnvelope(env)
	case pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE:
		d.met.AddGWUDPDataResponseRX(1)
		d.log.Debug("udp data response",
			zap.Uint64("transaction_id", env.GetTransactionId()),
			zap.String("from", addrString(from)),
			zap.Uint32("status", dataResponseStatus(env)),
			zap.ByteString("payload", dataResponsePayload(env)),
		)
		if err := d.tx.OnResponse(env); err != nil {
			txID := env.GetTransactionId()
			udp.ReleaseEnvelope(env)
			if shouldLogCompleteMiss(&completeMissLogNanos) {
				d.log.Warn("data response không khớp giao dịch",
					zap.Uint64("transaction_id", txID),
					zap.Error(err),
				)
			} else {
				d.log.Debug("data response không khớp giao dịch", zap.Uint64("transaction_id", txID), zap.Error(err))
			}
		}
		// success: ownership → Manager → httpsrv ReleaseEnvelope
	case pb.MessageType_MESSAGE_TYPE_ERROR:
		// Design §15: ERROR kết thúc waiter — không để timeout thành 504 giả.
		d.met.AddGWUDPDataResponseRX(1)
		d.log.Debug("udp error envelope",
			zap.Uint64("transaction_id", env.GetTransactionId()),
			zap.String("from", addrString(from)),
			zap.String("code", errorEnvelopeCode(env)),
			zap.String("message", errorEnvelopeMessage(env)),
		)
		if err := d.tx.OnResponse(env); err != nil {
			txID := env.GetTransactionId()
			udp.ReleaseEnvelope(env)
			if shouldLogCompleteMiss(&completeMissLogNanos) {
				d.log.Warn("error envelope không khớp giao dịch",
					zap.Uint64("transaction_id", txID),
					zap.Error(err),
				)
			} else {
				d.log.Debug("error envelope không khớp giao dịch", zap.Uint64("transaction_id", txID), zap.Error(err))
			}
		}
	case pb.MessageType_MESSAGE_TYPE_DATA_REQUEST:
		// Outbound HTTP có thể chậm — giữ goroutine riêng để không chiếm inbound worker.
		go func() {
			defer udp.ReleaseEnvelope(env)
			d.onDataRequest(ctx, env, from)
		}()
	default:
		d.log.Debug("bỏ qua loại envelope", zap.String("type", env.GetType().String()), zap.String("from", addrString(from)))
		d.sendError(ctx, from, env, pb.ErrorCode_ERROR_CODE_INVALID_MESSAGE, "unsupported message type")
		udp.ReleaseEnvelope(env)
	}
}

func (d *Dispatcher) onRegister(ctx context.Context, env *pb.Envelope, from *net.UDPAddr) {
	resp, err := d.reg.Register(env.GetRegisterRequest())
	if err != nil {
		d.log.Warn("đăng ký thất bại", zap.Error(err), zap.String("from", addrString(from)))
	} else if resp != nil && resp.GetAccepted() {
		d.met.AddLogicRegistered(1)
	}
	if resp == nil {
		return
	}
	out := protocol.Reply(d.nodeID, env, pb.MessageType_MESSAGE_TYPE_REGISTER_RESPONSE)
	out.Body = &pb.Envelope_RegisterResponse{RegisterResponse: resp}
	d.send(ctx, out, from)
}

func (d *Dispatcher) onHeartbeat(ctx context.Context, env *pb.Envelope, from *net.UDPAddr) {
	resp, err := d.hb.Handle(env.GetHeartbeatRequest())
	if err != nil {
		d.log.Debug("heartbeat bị từ chối", zap.Error(err), zap.String("from", addrString(from)))
		d.sendError(ctx, from, env, pb.ErrorCode_ERROR_CODE_NODE_NOT_AVAILABLE, err.Error())
		return
	}
	d.met.AddHeartbeatSuccess(1)
	out := protocol.Reply(d.nodeID, env, pb.MessageType_MESSAGE_TYPE_HEARTBEAT_RESPONSE)
	out.Body = &pb.Envelope_HeartbeatResponse{HeartbeatResponse: resp}
	d.send(ctx, out, from)
}

func (d *Dispatcher) onDataRequest(ctx context.Context, env *pb.Envelope, from *net.UDPAddr) {
	if d.ob == nil || !d.ob.Configured() {
		d.sendError(ctx, from, env, pb.ErrorCode_ERROR_CODE_NO_ROUTING_TARGET, "outbound remote URL is not configured")
		return
	}
	data, err := d.ob.Forward(ctx, env)
	if err != nil {
		d.log.Warn("outbound HTTP thất bại", zap.Error(err), zap.Uint64("transaction_id", env.GetTransactionId()))
		code := pb.ErrorCode_ERROR_CODE_NODE_NOT_AVAILABLE
		if errors.Is(err, context.DeadlineExceeded) || isTimeoutErr(err) {
			code = pb.ErrorCode_ERROR_CODE_TIMEOUT
		}
		d.sendError(ctx, from, env, code, err.Error())
		return
	}
	out := protocol.DataResponseEnvelope(d.nodeID, env, data.GetMessageId(), data.GetStatus(), data.GetPayload())
	d.send(ctx, out, from)
}

func (d *Dispatcher) send(ctx context.Context, env *pb.Envelope, to *net.UDPAddr) {
	if err := d.tr.Send(ctx, env, to); err != nil {
		d.log.Error("gửi UDP thất bại", zap.Error(err), zap.String("type", env.GetType().String()))
		return
	}
	d.met.AddUDPTx(1)
}

func (d *Dispatcher) sendError(ctx context.Context, to *net.UDPAddr, in *pb.Envelope, code pb.ErrorCode, message string) {
	if to == nil {
		return
	}
	d.send(ctx, protocol.ErrorEnvelope(d.nodeID, in, code, message), to)
}

func isTimeoutErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "timeout") || strings.Contains(s, "deadline exceeded")
}

func addrString(a *net.UDPAddr) string {
	if a == nil {
		return ""
	}
	return a.String()
}

func dataResponsePayload(env *pb.Envelope) []byte {
	if env == nil || env.GetDataResponse() == nil {
		return nil
	}
	return env.GetDataResponse().GetPayload()
}

func dataResponseStatus(env *pb.Envelope) uint32 {
	if env == nil || env.GetDataResponse() == nil {
		return 0
	}
	return env.GetDataResponse().GetStatus()
}

func heartbeatResponseSequence(env *pb.Envelope) uint64 {
	if env == nil || env.GetHeartbeatResponse() == nil {
		return 0
	}
	return env.GetHeartbeatResponse().GetSequence()
}

func errorEnvelopeCode(env *pb.Envelope) string {
	if env == nil || env.GetError() == nil {
		return ""
	}
	return env.GetError().GetCode().String()
}

func errorEnvelopeMessage(env *pb.Envelope) string {
	if env == nil || env.GetError() == nil {
		return ""
	}
	return env.GetError().GetMessage()
}

func envelopeType(env *pb.Envelope) string {
	if env == nil {
		return ""
	}
	return env.GetType().String()
}

func envelopeTxID(env *pb.Envelope) uint64 {
	if env == nil {
		return 0
	}
	return env.GetTransactionId()
}

// completeMissLogNanos / queueFullLogNanos rate-limit Warn (1/s).
var completeMissLogNanos atomic.Int64
var queueFullLogNanos atomic.Int64

func shouldLogCompleteMiss(last *atomic.Int64) bool {
	return rateLimitLog(last, time.Second)
}

func shouldLogQueueFull(last *atomic.Int64) bool {
	return rateLimitLog(last, time.Second)
}

func rateLimitLog(last *atomic.Int64, every time.Duration) bool {
	now := time.Now().UnixNano()
	prev := last.Load()
	if now-prev < int64(every) {
		return false
	}
	return last.CompareAndSwap(prev, now)
}

// Đảm bảo *outbound.Client thỏa Outbound.
var _ Outbound = (*outbound.Client)(nil)
