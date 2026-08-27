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

// maxRouteAttempts: lần 1 + tối đa 1 lần failover (exclude node vừa fail).
const maxRouteAttempts = 2

// Router abstraction tối thiểu; không phụ thuộc concrete *router.Router.
type Router interface {
	Route(ctx context.Context, env *pb.Envelope) (*registry.Node, error)
}

// excludingRouter — optional; *router.Router implement.
type excludingRouter interface {
	RouteExcluding(ctx context.Context, env *pb.Envelope, excludeNodeIDs ...uint32) (*registry.Node, error)
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
// Nếu Send thất bại hoặc Wait timeout: thử lại tối đa 1 lần, exclude node vừa fail (Active-Active failover).
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

	log := txLog()
	var exclude []uint32
	var lastErr error

	for attempt := 0; attempt < maxRouteAttempts; attempt++ {
		if err := reqCtx.Err(); err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, err
		}

		createID := uint64(0)
		if attempt == 0 {
			createID = req.GetTransactionId()
		}
		id, err := s.mgr.Create(createID)
		if err != nil {
			return nil, err
		}
		req.TransactionId = id
		log.Debug("tx create",
			zap.Uint64("transaction_id", id),
			zap.Int("attempt", attempt+1),
			zap.Uint32("message_id", req.GetDataRequest().GetMessageId()),
			zap.String("session_id", req.GetDataRequest().GetSessionId()),
			zap.String("trace_id", req.GetTraceId()),
			zap.Int("payload_bytes", len(req.GetDataRequest().GetPayload())),
		)

		node, err := s.route(reqCtx, req, exclude...)
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
		log.Debug("tx route", zap.Uint64("transaction_id", id), zap.Uint32("node_id", node.ID), zap.String("udp", addr.String()), zap.Int("attempt", attempt+1))

		if err := s.transport.Send(reqCtx, req, addr); err != nil {
			log.Debug("tx send thất bại", zap.Uint64("transaction_id", id), zap.String("udp", addr.String()), zap.Error(err))
			_ = s.mgr.Cancel(id)
			lastErr = err
			exclude = appendUnique(exclude, node.ID)
			if attempt+1 < maxRouteAttempts {
				log.Info("tx failover sau send fail", zap.Uint32("exclude_node_id", node.ID), zap.Int("next_attempt", attempt+2))
				continue
			}
			return nil, err
		}
		log.Debug("tx send", zap.Uint64("transaction_id", id), zap.String("udp", addr.String()))

		waitCtx := reqCtx
		var waitCancel context.CancelFunc
		if attempt == 0 && maxRouteAttempts > 1 {
			half := s.timeout / 2
			if half >= 50*time.Millisecond {
				waitCtx, waitCancel = context.WithTimeout(reqCtx, half)
			}
		}
		env, err := s.mgr.Wait(waitCtx, id)
		if waitCancel != nil {
			waitCancel()
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				log.Debug("tx timeout", zap.Uint64("transaction_id", id), zap.Uint32("node_id", node.ID))
				lastErr = ErrTransactionTimeout
				exclude = appendUnique(exclude, node.ID)
				if attempt+1 < maxRouteAttempts && reqCtx.Err() == nil {
					log.Info("tx failover sau timeout", zap.Uint32("exclude_node_id", node.ID), zap.Int("next_attempt", attempt+2))
					continue
				}
				return nil, ErrTransactionTimeout
			}
			log.Debug("tx wait thất bại", zap.Uint64("transaction_id", id), zap.Error(err))
			return nil, err
		}
		log.Debug("tx complete", zap.Uint64("transaction_id", id), zap.Uint32("status", dataStatus(env)), zap.Int("attempt", attempt+1))
		return env, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, ErrTransactionTimeout
}

func (s *Service) route(ctx context.Context, req *pb.Envelope, exclude ...uint32) (*registry.Node, error) {
	if len(exclude) > 0 {
		if er, ok := s.router.(excludingRouter); ok {
			return er.RouteExcluding(ctx, req, exclude...)
		}
	}
	return s.router.Route(ctx, req)
}

func appendUnique(ids []uint32, id uint32) []uint32 {
	for _, x := range ids {
		if x == id {
			return ids
		}
	}
	return append(ids, id)
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
