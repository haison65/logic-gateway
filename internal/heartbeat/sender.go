package heartbeat

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/haison65/logic-gateway/internal/logger"
	"github.com/haison65/logic-gateway/internal/protocol"
	"github.com/haison65/logic-gateway/internal/registry"
	"github.com/haison65/logic-gateway/internal/transport/udp"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
	"go.uber.org/zap"
)

// Sender gửi HEARTBEAT_REQUEST từ HTTP2GW tới các Logic đã đăng ký (không DEAD).
// Logic trả HEARTBEAT_RESPONSE; Dispatcher phía gateway chỉ nhận (không ApplyHeartbeat).
type Sender struct {
	nodeID   uint32
	registry registry.Registry
	tr       udp.Transport
	interval time.Duration
	clock    Clock
	log      *zap.Logger
	seq      atomic.Uint64
}

// NewSender tạo sender. interval phải > 0; reg và tr không nil.
func NewSender(nodeID uint32, reg registry.Registry, tr udp.Transport, interval time.Duration, clock Clock, log *zap.Logger) (*Sender, error) {
	if nodeID == 0 {
		return nil, fmt.Errorf("%w: node_id must be > 0", ErrInvalidConfig)
	}
	if reg == nil {
		return nil, fmt.Errorf("%w: registry is required", ErrInvalidConfig)
	}
	if tr == nil {
		return nil, fmt.Errorf("%w: transport is required", ErrInvalidConfig)
	}
	if interval <= 0 {
		return nil, fmt.Errorf("%w: interval must be > 0", ErrInvalidConfig)
	}
	return &Sender{
		nodeID:   nodeID,
		registry: reg,
		tr:       tr,
		interval: interval,
		clock:    resolveClock(clock),
		log:      logger.OrNop(log),
	}, nil
}

// Run gửi heartbeat định kỳ đến khi ctx hủy.
func (s *Sender) Run(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("%w: sender is nil", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Sender) tick(ctx context.Context) {
	for _, n := range s.registry.List() {
		if n == nil || n.Type != registry.TypeLogic {
			continue
		}
		if n.State == registry.NodeStateDead || n.State == registry.NodeStateUnspecified {
			continue
		}
		addr, err := logicUDPAddr(n)
		if err != nil {
			s.log.Debug("heartbeat outbound bỏ qua node", zap.Uint32("node_id", n.ID), zap.Error(err))
			continue
		}
		if err := s.sendOne(ctx, n.ID, addr); err != nil {
			s.log.Debug("gửi HEARTBEAT tới Logic thất bại",
				zap.Uint32("node_id", n.ID),
				zap.String("udp", addr.String()),
				zap.Error(err),
			)
		}
	}
}

func (s *Sender) sendOne(ctx context.Context, destNodeID uint32, addr *net.UDPAddr) error {
	n := s.seq.Add(1)
	ts := s.clock.Now().UnixMilli()
	if ts < 0 {
		ts = 0
	}
	env := protocol.Reply(s.nodeID, nil, pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST)
	env.DestinationNodeId = destNodeID
	env.TransactionId = n
	env.Body = &pb.Envelope_HeartbeatRequest{HeartbeatRequest: &pb.HeartbeatRequest{
		NodeId:      s.nodeID,
		Sequence:    n,
		TimestampMs: uint64(ts),
	}}
	return s.tr.Send(ctx, env, addr)
}

func logicUDPAddr(n *registry.Node) (*net.UDPAddr, error) {
	if n == nil {
		return nil, fmt.Errorf("node is nil")
	}
	ip := net.ParseIP(n.Address)
	if ip == nil {
		return nil, fmt.Errorf("address %q", n.Address)
	}
	if n.UDPPort == 0 || n.UDPPort > 65535 {
		return nil, fmt.Errorf("port %d", n.UDPPort)
	}
	return &net.UDPAddr{IP: ip, Port: int(n.UDPPort)}, nil
}
