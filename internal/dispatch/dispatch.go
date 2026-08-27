package dispatch

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/haison65/logic-gateway/internal/logger"
	"github.com/haison65/logic-gateway/internal/metrics"
	"github.com/haison65/logic-gateway/internal/outbound"
	"github.com/haison65/logic-gateway/internal/protocol"
	"github.com/haison65/logic-gateway/internal/transport/udp"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
	"go.uber.org/zap"
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

// Dispatcher là consumer duy nhất của Transport.Receive.
type Dispatcher struct {
	nodeID uint32
	tr     udp.Transport
	reg    Registrar
	hb     Heartbeater
	tx     Completer
	ob     Outbound
	met    *metrics.Metrics
	log    *zap.Logger
}

// New tạo Dispatcher. tr, reg, hb, tx không được nil. ob và met được phép nil.
func New(nodeID uint32, tr udp.Transport, reg Registrar, hb Heartbeater, tx Completer, log *zap.Logger, ob Outbound, met *metrics.Metrics) (*Dispatcher, error) {
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
	log = logger.OrNop(log)
	return &Dispatcher{nodeID: nodeID, tr: tr, reg: reg, hb: hb, tx: tx, ob: ob, met: met, log: log}, nil
}

// Run đọc UDP cho đến khi ctx hủy hoặc transport đóng.
func (d *Dispatcher) Run(ctx context.Context) error {
	if d == nil {
		return fmt.Errorf("dispatcher is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		env, from, err := d.tr.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, udp.ErrTransportClosed) {
				return err
			}
			if errors.Is(err, udp.ErrInvalidMessage) {
				d.met.AddUDPRx(1)
				d.log.Warn("udp envelope không hợp lệ", zap.Error(err), zap.String("from", addrString(from)))
				d.sendError(ctx, from, nil, pb.ErrorCode_ERROR_CODE_INVALID_MESSAGE, err.Error())
				continue
			}
			return err
		}
		d.met.AddUDPRx(1)
		d.handle(ctx, env, from)
	}
}

func (d *Dispatcher) handle(ctx context.Context, env *pb.Envelope, from *net.UDPAddr) {
	if env == nil {
		return
	}
	switch env.GetType() {
	case pb.MessageType_MESSAGE_TYPE_REGISTER_REQUEST:
		d.onRegister(ctx, env, from)
	case pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST:
		d.onHeartbeat(ctx, env, from)
	case pb.MessageType_MESSAGE_TYPE_HEARTBEAT_RESPONSE:
		// Phản hồi cho HEARTBEAT HTTP2GW → Logic; không ApplyHeartbeat (Logic đã sống qua chiều ngược).
		d.log.Debug("udp heartbeat response",
			zap.Uint64("transaction_id", env.GetTransactionId()),
			zap.String("from", addrString(from)),
			zap.Uint64("sequence", heartbeatResponseSequence(env)),
		)
	case pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE:
		d.log.Debug("udp data response",
			zap.Uint64("transaction_id", env.GetTransactionId()),
			zap.String("from", addrString(from)),
			zap.Uint32("status", dataResponseStatus(env)),
			zap.ByteString("payload", dataResponsePayload(env)),
		)
		if err := d.tx.OnResponse(env); err != nil {
			d.log.Debug("data response không khớp giao dịch", zap.Uint64("transaction_id", env.GetTransactionId()), zap.Error(err))
		}
	case pb.MessageType_MESSAGE_TYPE_DATA_REQUEST:
		// Một Receive consumer: HTTP chạy goroutine, dùng ctx của Dispatcher.Run.
		// Hủy ctx (shutdown) hủy Forward; không WaitGroup — HTTP Client.Timeout + req context đủ thoát.
		go d.onDataRequest(ctx, env, from)
	default:
		d.log.Debug("bỏ qua loại envelope", zap.String("type", env.GetType().String()), zap.String("from", addrString(from)))
		d.sendError(ctx, from, env, pb.ErrorCode_ERROR_CODE_INVALID_MESSAGE, "unsupported message type")
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

// Đảm bảo *outbound.Client thỏa Outbound.
var _ Outbound = (*outbound.Client)(nil)
