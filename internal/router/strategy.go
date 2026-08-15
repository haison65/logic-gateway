package router

import "github.com/haison65/logic-gateway/internal/registry"

// Strategy chọn một node trong danh sách đã đủ điều kiện.
// Không biết UDP, heartbeat hay protobuf.
type Strategy interface {
	Select(nodes []*registry.Node, key string) (*registry.Node, error)
}
