package transaction

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/haison65/logic-gateway/internal/registry"
	"github.com/haison65/logic-gateway/internal/transport/udp"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
	"go.uber.org/zap"
)

// Router abstraction tối thiểu; không phụ thuộc concrete *router.Router.
type Router interface {
	Route(ctx context.Context, env *pb.Envelope) (*registry.Node, error)
}

// Service ghép Create → Route → Send → Wait. Không gọi Transport.Receive.
type Service struct {
	mgr       *Manager
	router    Router
	transport udp.Transport
	timeout   time.Duration
}

// NewService tạo Service. timeout <= 0 thì DefaultTimeout.
func NewService(mgr *Manager, rt Router, tr udp.Transport, cfg Config) (*Service, error) {
	if mgr == nil {
		return nil, fmt.Errorf("%w: manager is required", ErrInvalidTransaction)
	}
	if rt == nil {
		return nil, fmt.Errorf("%w: router is required", ErrInvalidTransaction)
	}
	if tr == nil {
		return nil, fmt.Errorf("%w: transport is required", ErrInvalidTransaction)
	}
	if cfg.Timeout < 0 {
		return nil, fmt.Errorf("%w: timeout must be > 0", ErrInvalidTransaction)
	}
	return &Service{
		mgr:       mgr,
		router:    rt,
		transport: tr,
		timeout:   cfg.timeoutOrDefault(),
	}, nil
}

// Request gửi DATA_REQUEST và chờ DATA_RESPONSE theo transaction_id.
func (s *Service) Request(ctx context.Context, req *pb.Envelope) (*pb.Envelope, error) {
	if s == nil || s.mgr == nil || s.router == nil || s.transport == nil {
		return nil, fmt.Errorf("%w: service is not configured", ErrInvalidRequest)
	}
	if req == nil || req.GetType() != pb.MessageType_MESSAGE_TYPE_DATA_REQUEST || req.GetDataRequest() == nil {
		return nil, fmt.Errorf("%w: require DATA_REQUEST", ErrInvalidRequest)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	id, err := s.mgr.Create(req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	req.TransactionId = id
	log := txLog()
	log.Debug("tx create",
		zap.Uint64("transaction_id", id),
		zap.Uint32("message_id", req.GetDataRequest().GetMessageId()),
		zap.String("session_id", req.GetDataRequest().GetSessionId()),
		zap.String("trace_id", req.GetTraceId()),
		zap.Int("payload_bytes", len(req.GetDataRequest().GetPayload())),
	)

	node, err := s.router.Route(reqCtx, req)
	if err != nil {
		log.Debug("tx route thất bại", zap.Uint64("transaction_id", id), zap.Error(err))
		_ = s.mgr.Cancel(id)
		return nil, err
	}
	req.DestinationNodeId = node.ID
	addr, err := udpAddr(node)
	if err != nil {
		log.Debug("tx addr thất bại", zap.Uint64("transaction_id", id), zap.Uint32("node_id", node.ID), zap.Error(err))
		_ = s.mgr.Cancel(id)
		return nil, err
	}
	log.Debug("tx route", zap.Uint64("transaction_id", id), zap.Uint32("node_id", node.ID), zap.String("udp", addr.String()))
	if err := s.transport.Send(reqCtx, req, addr); err != nil {
		log.Debug("tx send thất bại", zap.Uint64("transaction_id", id), zap.String("udp", addr.String()), zap.Error(err))
		_ = s.mgr.Cancel(id)
		return nil, err
	}
	log.Debug("tx send", zap.Uint64("transaction_id", id), zap.String("udp", addr.String()))

	env, err := s.mgr.Wait(reqCtx, id)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Debug("tx timeout", zap.Uint64("transaction_id", id))
			return nil, ErrTransactionTimeout
		}
		log.Debug("tx wait thất bại", zap.Uint64("transaction_id", id), zap.Error(err))
		return nil, err
	}
	log.Debug("tx complete", zap.Uint64("transaction_id", id), zap.Uint32("status", dataStatus(env)))
	return env, nil
}

// OnResponse chỉ nhận DATA_RESPONSE rồi Complete theo transaction_id.
func (s *Service) OnResponse(env *pb.Envelope) error {
	if s == nil || s.mgr == nil {
		return fmt.Errorf("%w: service is not configured", ErrInvalidRequest)
	}
	if env == nil || env.GetType() != pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE {
		return fmt.Errorf("%w: require DATA_RESPONSE", ErrInvalidResponse)
	}
	if env.GetTransactionId() == 0 {
		return fmt.Errorf("%w: missing transaction_id", ErrInvalidResponse)
	}
	err := s.mgr.Complete(env.GetTransactionId(), env)
	if err != nil {
		txLog().Debug("tx on_response thất bại", zap.Uint64("transaction_id", env.GetTransactionId()), zap.Error(err))
		return err
	}
	txLog().Debug("tx on_response", zap.Uint64("transaction_id", env.GetTransactionId()), zap.Uint32("status", dataStatus(env)))
	return nil
}

func txLog() *zap.Logger {
	return zap.L().Named("transaction")
}

func dataStatus(env *pb.Envelope) uint32 {
	if env == nil || env.GetDataResponse() == nil {
		return 0
	}
	return env.GetDataResponse().GetStatus()
}

// Close đóng Manager, giải phóng waiter.
func (s *Service) Close() error {
	if s == nil || s.mgr == nil {
		return nil
	}
	return s.mgr.Close()
}

func udpAddr(n *registry.Node) (*net.UDPAddr, error) {
	if n == nil {
		return nil, fmt.Errorf("%w: nil node", ErrInvalidNode)
	}
	ip := net.ParseIP(n.Address)
	if ip == nil {
		return nil, fmt.Errorf("%w: address %q", ErrInvalidNode, n.Address)
	}
	if n.UDPPort == 0 || n.UDPPort > 65535 {
		return nil, fmt.Errorf("%w: port %d", ErrInvalidNode, n.UDPPort)
	}
	return &net.UDPAddr{IP: ip, Port: int(n.UDPPort)}, nil
}
