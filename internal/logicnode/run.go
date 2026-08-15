package logicnode

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/haison65/logic-gateway/internal/config"
	"github.com/haison65/logic-gateway/internal/logger"
	"github.com/haison65/logic-gateway/internal/netaddr"
	"github.com/haison65/logic-gateway/internal/protocol"
	"github.com/haison65/logic-gateway/internal/transport/udp"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
	"go.uber.org/zap"
)

const (
	envelopeVersion   = 1
	messageCreateCall = 1001
	messageUpdateCall = 1002
	messageDeleteCall = 1003
)

type runtimeStats struct {
	inFlight     atomic.Int64
	heartbeatRTT time.Duration
	rttMu        sync.Mutex
	hbSentAt     atomic.Int64
}

func (s *runtimeStats) queueSize() int64 {
	if s == nil {
		return 0
	}
	return s.inFlight.Load()
}

// Run lắng UDP, REGISTER với gateway, HEARTBEAT định kỳ, echo DATA_RESPONSE.
// Receive chỉ chạy trên một goroutine.
func Run(ctx context.Context, cfg config.Logic, log *zap.Logger) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	log = logger.OrNop(log)

	conn, err := udp.New(udp.Config{
		ListenHost: cfg.UDP.Listen,
		ListenPort: cfg.UDP.Port,
	})
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	gwAddr, err := netaddr.ResolveUDPAddr(ctx, cfg.Gateway.Host, cfg.Gateway.Port)
	if err != nil {
		return fmt.Errorf("gateway.host: %w", err)
	}

	local, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || local == nil {
		return fmt.Errorf("logic local UDP address is invalid")
	}
	advertiseHost := cfg.AdvertiseHost()
	advIP, err := netaddr.ResolveIP(ctx, advertiseHost)
	if err != nil {
		return fmt.Errorf("node advertise: %w", err)
	}
	advertiseIP := advIP.String()
	advertisePort := uint32(local.Port)
	log.Info("logic đang lắng nghe UDP",
		zap.Stringer("addr", local),
		zap.String("advertise_host", advertiseHost),
		zap.String("advertise", net.JoinHostPort(advertiseIP, fmt.Sprintf("%d", advertisePort))),
		zap.String("gateway", gwAddr.String()),
	)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rt := &runtimeStats{}
	regCh := make(chan *pb.RegisterResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		err := receiveLoop(runCtx, conn, cfg.Node.NodeID, gwAddr, log, regCh, rt)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, udp.ErrTransportClosed) {
			errCh <- err
			cancel()
		}
	}()

	if err := registerUntilAccepted(runCtx, conn, cfg, advertiseIP, advertisePort, gwAddr, regCh, log); err != nil {
		_ = conn.Close()
		return err
	}

	var seq atomic.Uint64
	go heartbeatLoop(runCtx, conn, cfg, gwAddr, &seq, log, rt)

	select {
	case <-runCtx.Done():
		_ = conn.Close()
		return ctx.Err()
	case err := <-errCh:
		_ = conn.Close()
		return err
	}
}

func registerUntilAccepted(
	ctx context.Context,
	tr udp.Transport,
	cfg config.Logic,
	ip string,
	port uint32,
	gw *net.UDPAddr,
	regCh <-chan *pb.RegisterResponse,
	log *zap.Logger,
) error {
	interval := cfg.Heartbeat.Interval.Duration()
	if interval <= 0 {
		interval = time.Second
	}
	if err := sendRegister(ctx, tr, cfg, ip, port, gw); err != nil {
		return err
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp := <-regCh:
			if resp != nil && resp.GetAccepted() {
				log.Info("logic đã đăng ký", zap.Uint32("node_id", resp.GetAssignedNodeId()), zap.String("message", resp.GetMessage()))
				return nil
			}
			log.Warn("REGISTER bị từ chối, thử lại", zap.String("message", respMessage(resp)))
		case <-timer.C:
			log.Info("gửi lại REGISTER")
		}
		if err := sendRegister(ctx, tr, cfg, ip, port, gw); err != nil {
			return err
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(interval)
	}
}

func sendRegister(ctx context.Context, tr udp.Transport, cfg config.Logic, ip string, port uint32, gw *net.UDPAddr) error {
	services := make([]*pb.ServiceCapability, 0, len(cfg.Services))
	for _, svc := range cfg.Services {
		services = append(services, &pb.ServiceCapability{
			ServiceId:    svc.ServiceID,
			ServiceName:  svc.ServiceName,
			MessageTypes: append([]uint32(nil), svc.MessageTypes...),
		})
	}
	env := envelope(cfg.Node.NodeID, 0, pb.MessageType_MESSAGE_TYPE_REGISTER_REQUEST)
	env.Body = &pb.Envelope_RegisterRequest{RegisterRequest: &pb.RegisterRequest{
		NodeId:     cfg.Node.NodeID,
		NodeType:   pb.NodeType_NODE_TYPE_LOGIC,
		InstanceId: cfg.Node.InstanceID,
		Ip:         ip,
		Port:       port,
		Services:   services,
	}}
	return tr.Send(ctx, env, gw)
}

func heartbeatLoop(ctx context.Context, tr udp.Transport, cfg config.Logic, gw *net.UDPAddr, seq *atomic.Uint64, log *zap.Logger, rt *runtimeStats) {
	interval := cfg.Heartbeat.Interval.Duration()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	sendHeartbeat(ctx, tr, cfg.Node.NodeID, gw, seq, rt)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := sendHeartbeat(ctx, tr, cfg.Node.NodeID, gw, seq, rt); err != nil {
				log.Warn("gửi HEARTBEAT thất bại", zap.Error(err))
			}
		}
	}
}

func sendHeartbeat(ctx context.Context, tr udp.Transport, nodeID uint32, gw *net.UDPAddr, seq *atomic.Uint64, rt *runtimeStats) error {
	n := seq.Add(1)
	ts := time.Now().UnixMilli()
	if ts < 0 {
		ts = 0
	}
	var load, active uint32
	if rt != nil {
		q := rt.queueSize()
		if q < 0 {
			q = 0
		}
		load = uint32(q)
		active = uint32(q)
		rt.hbSentAt.Store(time.Now().UnixNano())
	}
	env := envelope(nodeID, 0, pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST)
	env.TransactionId = n
	env.Body = &pb.Envelope_HeartbeatRequest{HeartbeatRequest: &pb.HeartbeatRequest{
		NodeId:            nodeID,
		Sequence:          n,
		TimestampMs:       uint64(ts),
		Load:              load,
		ActiveTransaction: active,
	}}
	return tr.Send(ctx, env, gw)
}

func receiveLoop(ctx context.Context, tr udp.Transport, nodeID uint32, gw *net.UDPAddr, log *zap.Logger, regCh chan<- *pb.RegisterResponse, rt *runtimeStats) error {
	for {
		env, from, err := tr.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, udp.ErrTransportClosed) {
				return err
			}
			if errors.Is(err, udp.ErrInvalidMessage) {
				log.Warn("udp envelope không hợp lệ", zap.Error(err))
				continue
			}
			return err
		}
		handleIncoming(ctx, tr, nodeID, gw, log, regCh, env, from, rt)
	}
}

func handleIncoming(ctx context.Context, tr udp.Transport, nodeID uint32, gw *net.UDPAddr, log *zap.Logger, regCh chan<- *pb.RegisterResponse, env *pb.Envelope, from *net.UDPAddr, rt *runtimeStats) {
	if env == nil {
		return
	}
	switch env.GetType() {
	case pb.MessageType_MESSAGE_TYPE_REGISTER_RESPONSE:
		select {
		case regCh <- env.GetRegisterResponse():
		default:
		}
	case pb.MessageType_MESSAGE_TYPE_HEARTBEAT_RESPONSE:
		if rt != nil {
			sent := rt.hbSentAt.Load()
			if sent > 0 {
				rt.rttMu.Lock()
				rt.heartbeatRTT = time.Duration(time.Now().UnixNano() - sent)
				rt.rttMu.Unlock()
			}
		}
		return
	case pb.MessageType_MESSAGE_TYPE_DATA_REQUEST:
		dest := from
		if dest == nil {
			dest = gw
		}
		if rt != nil {
			rt.inFlight.Add(1)
			defer rt.inFlight.Add(-1)
		}
		in := env.GetDataRequest()
		var msgID uint32
		var n int
		if in != nil {
			msgID = in.GetMessageId()
			n = len(in.GetPayload())
		}
		log.Debug("udp data request",
			zap.Uint64("transaction_id", env.GetTransactionId()),
			zap.Uint32("message_id", msgID),
			zap.Int("payload_bytes", n),
			zap.String("from", addrString(from)),
			zap.String("trace_id", env.GetTraceId()),
		)
		out := handleBusiness(nodeID, env, msgID)
		if err := tr.Send(ctx, out, dest); err != nil {
			log.Error("gửi DATA/ERROR thất bại", zap.Error(err), zap.Uint64("transaction_id", env.GetTransactionId()))
			return
		}
		log.Debug("udp data out", zap.Uint64("transaction_id", env.GetTransactionId()), zap.Uint32("message_id", msgID), zap.String("type", out.GetType().String()), zap.String("to", addrString(dest)))
	default:
		log.Debug("bỏ qua loại envelope", zap.String("type", env.GetType().String()))
	}
}

func handleBusiness(nodeID uint32, req *pb.Envelope, msgID uint32) *pb.Envelope {
	switch msgID {
	case messageCreateCall, messageUpdateCall, messageDeleteCall:
		return echoResponse(nodeID, req)
	default:
		return protocol.ErrorEnvelope(nodeID, req, pb.ErrorCode_ERROR_CODE_INVALID_MESSAGE, fmt.Sprintf("unsupported message_id %d", msgID))
	}
}

func echoResponse(nodeID uint32, req *pb.Envelope) *pb.Envelope {
	in := req.GetDataRequest()
	var msgID uint32
	var payload []byte
	if in != nil {
		msgID = in.GetMessageId()
		payload = append([]byte(nil), in.GetPayload()...)
	}
	out := envelope(nodeID, req.GetSourceNodeId(), pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE)
	out.TransactionId = req.GetTransactionId()
	out.TraceId = req.GetTraceId()
	out.Body = &pb.Envelope_DataResponse{DataResponse: &pb.DataResponse{
		MessageId: msgID,
		Status:    200,
		Payload:   payload,
	}}
	return out
}

func envelope(src, dst uint32, typ pb.MessageType) *pb.Envelope {
	ts := time.Now().UnixMilli()
	if ts < 0 {
		ts = 0
	}
	return &pb.Envelope{
		Version:           envelopeVersion,
		Type:              typ,
		SourceNodeId:      src,
		DestinationNodeId: dst,
		TimestampMs:       uint64(ts),
	}
}

func respMessage(resp *pb.RegisterResponse) string {
	if resp == nil {
		return ""
	}
	return resp.GetMessage()
}

func addrString(a *net.UDPAddr) string {
	if a == nil {
		return ""
	}
	return a.String()
}
