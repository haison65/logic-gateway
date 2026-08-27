// Package router chọn Logic node ACTIVE phù hợp để nhận Envelope DATA.
//
// Router không gửi UDP, không sửa health, không đăng ký node.
package router

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/haison65/logic-gateway/internal/registry"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

// NodeSource cung cấp danh sách node hiện tại. Registry.List đủ dùng.
type NodeSource interface {
	List() []*registry.Node
}

// Router lọc candidate rồi ủy quyền cho Strategy.
type Router struct {
	source   NodeSource
	strategy Strategy
}

// New tạo Router. source và strategy không được nil.
func New(source NodeSource, strategy Strategy) *Router {
	return &Router{source: source, strategy: strategy}
}

// Route trả về bản sao node đích. Không gửi message.
func (r *Router) Route(ctx context.Context, env *pb.Envelope) (*registry.Node, error) {
	return r.RouteExcluding(ctx, env)
}

// RouteExcluding như Route nhưng bỏ qua các node_id (vd failover sau Send/timeout).
func (r *Router) RouteExcluding(ctx context.Context, env *pb.Envelope, excludeNodeIDs ...uint32) (*registry.Node, error) {
	if r == nil || r.source == nil || r.strategy == nil {
		return nil, fmt.Errorf("%w: router is not configured", ErrNoEligibleNode)
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if env == nil {
		return nil, fmt.Errorf("%w: nil envelope", ErrUnsupportedMessageType)
	}

	msgID, err := requestedMessageID(env)
	if err != nil {
		return nil, err
	}

	exclude := make(map[uint32]struct{}, len(excludeNodeIDs))
	for _, id := range excludeNodeIDs {
		if id != 0 {
			exclude[id] = struct{}{}
		}
	}

	candidates := eligible(r.source.List(), msgID)
	if len(exclude) > 0 {
		filtered := candidates[:0]
		for _, n := range candidates {
			if _, skip := exclude[n.ID]; skip {
				continue
			}
			filtered = append(filtered, n)
		}
		candidates = filtered
	}
	if len(candidates) == 0 {
		return nil, ErrNoEligibleNode
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })

	return r.strategy.Select(candidates, routingKey(env))
}

func requestedMessageID(env *pb.Envelope) (uint32, error) {
	switch env.GetType() {
	case pb.MessageType_MESSAGE_TYPE_DATA_REQUEST:
		body := env.GetDataRequest()
		if body == nil {
			return 0, fmt.Errorf("%w: missing data_request", ErrUnsupportedMessageType)
		}
		return body.GetMessageId(), nil
	case pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE:
		body := env.GetDataResponse()
		if body == nil {
			return 0, fmt.Errorf("%w: missing data_response", ErrUnsupportedMessageType)
		}
		return body.GetMessageId(), nil
	default:
		return 0, fmt.Errorf("%w: %s", ErrUnsupportedMessageType, env.GetType())
	}
}

// routingKey ưu tiên session_id (dính phiên); không có thì dùng transaction_id.
func routingKey(env *pb.Envelope) string {
	if dr := env.GetDataRequest(); dr != nil {
		if sid := dr.GetSessionId(); sid != "" {
			return sid
		}
	}
	if env.GetTransactionId() != 0 {
		return strconv.FormatUint(env.GetTransactionId(), 10)
	}
	return ""
}

func eligible(nodes []*registry.Node, msgID uint32) []*registry.Node {
	out := make([]*registry.Node, 0, len(nodes))
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if n.State != registry.NodeStateActive {
			continue
		}
		if n.Type != registry.TypeLogic {
			continue
		}
		if supports(n, msgID) {
			out = append(out, n)
		}
	}
	return out
}

func supports(n *registry.Node, msgID uint32) bool {
	for _, svc := range n.Services {
		for _, t := range svc.MessageTypes {
			if t == msgID {
				return true
			}
		}
	}
	return false
}
