package router

import (
	"sync/atomic"

	"github.com/haison65/logic-gateway/internal/registry"
)

// RoundRobin xoay vòng trên danh sách candidate (thứ tự do Router sắp xếp).
type RoundRobin struct {
	n atomic.Uint64
}

// NewRoundRobin tạo strategy round-robin với bộ đếm riêng (không dùng biến global).
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

// Select trả về node tiếp theo. Bỏ qua key.
func (r *RoundRobin) Select(nodes []*registry.Node, _ string) (*registry.Node, error) {
	if r == nil {
		return nil, ErrEmptyCandidates
	}
	if len(nodes) == 0 {
		return nil, ErrEmptyCandidates
	}
	i := r.n.Add(1) - 1
	return nodes[i%uint64(len(nodes))], nil
}
