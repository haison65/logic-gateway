package registry

import (
	"fmt"
	"sync"
	"time"
)

// Registry lưu và truy xuất node đã đăng ký.
// Không chứa API định tuyến.
type Registry interface {
	Register(node *Node) error
	Get(nodeID uint32) (*Node, bool)
	Remove(nodeID uint32) error
	List() []*Node
	// ApplyHeartbeat ghi nhận heartbeat và chuyển trạng thái dưới cùng một lock.
	// transition nhận state hiện tại và trả về state mới hoặc lỗi (ví dụ DEAD).
	ApplyHeartbeat(nodeID uint32, at time.Time, transition func(NodeState) (NodeState, error)) (*Node, error)
	// ApplyTimeouts áp dụng chuyển timeout cho mọi node dưới cùng một lock.
	// transition trả về (state mới, changed).
	ApplyTimeouts(now time.Time, transition func(state NodeState, lastHeartbeat time.Time, now time.Time) (NodeState, bool)) []*Node
	SetRuntime(nodeID uint32, load, activeTransaction uint32) error
}

// Memory là Registry in-memory, an toàn khi dùng đồng thời.
//
// Get và List trả về bản sao của Node (kèm clone slice Services),
// nên caller không thể làm hỏng trạng thái nội bộ.
type Memory struct {
	mu    sync.RWMutex
	nodes map[uint32]*Node
}

// NewMemory tạo registry rỗng.
func NewMemory() *Memory {
	return &Memory{nodes: make(map[uint32]*Node)}
}

// Register thêm hoặc cập nhật node theo ID.
// Cùng một node_id đăng ký lại sẽ ghi đè metadata, đặt State = Registered
// và LastHeartbeatAt = now. Đây là cơ chế phục hồi tường minh (kể cả từ DEAD).
func (m *Memory) Register(node *Node) error {
	if node == nil || node.ID == 0 {
		return fmt.Errorf("%w: node_id is required", ErrInvalidNode)
	}

	stored := cloneNode(node)
	// Register hoàn tất INIT → REGISTERED trong một bước (design §8).
	stored.State = NodeStateRegistered
	stored.LastHeartbeatAt = time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[stored.ID] = stored
	return nil
}

// Get trả về bản sao node nếu tồn tại.
func (m *Memory) Get(nodeID uint32) (*Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.nodes[nodeID]
	if !ok {
		return nil, false
	}
	return cloneNode(n), true
}

// Remove xóa node. Node không tồn tại trả về ErrNotFound.
func (m *Memory) Remove(nodeID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.nodes[nodeID]; !ok {
		return ErrNotFound
	}
	delete(m.nodes, nodeID)
	return nil
}

// List trả về bản sao tất cả node. Thứ tự không được đảm bảo.
func (m *Memory) List() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, cloneNode(n))
	}
	return out
}

// ApplyHeartbeat cập nhật LastHeartbeatAt và state một cách nguyên tử.
func (m *Memory) ApplyHeartbeat(nodeID uint32, at time.Time, transition func(NodeState) (NodeState, error)) (*Node, error) {
	if nodeID == 0 {
		return nil, fmt.Errorf("%w: node_id is required", ErrInvalidNode)
	}
	if transition == nil {
		return nil, fmt.Errorf("%w: missing heartbeat transition", ErrInvalidNode)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[nodeID]
	if !ok {
		return nil, ErrNotFound
	}
	next, err := transition(n.State)
	if err != nil {
		return cloneNode(n), err
	}
	n.State = next
	n.LastHeartbeatAt = at
	return cloneNode(n), nil
}

// ApplyTimeouts chuyển trạng thái timeout một cách nguyên tử cho mọi node.
// Bỏ qua TypeHTTP2GW (DATA-plane registry trên gateway không timeout chính nó).
func (m *Memory) ApplyTimeouts(now time.Time, transition func(state NodeState, lastHeartbeat time.Time, now time.Time) (NodeState, bool)) []*Node {
	return m.ApplyTimeoutsExcept(now, func(n *Node) bool {
		return n != nil && n.Type == TypeHTTP2GW
	}, transition)
}

// ApplyTimeoutsExcept như ApplyTimeouts nhưng skip theo predicate (vd Master bỏ qua chính nó).
func (m *Memory) ApplyTimeoutsExcept(now time.Time, skip func(*Node) bool, transition func(state NodeState, lastHeartbeat time.Time, now time.Time) (NodeState, bool)) []*Node {
	if transition == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	changed := make([]*Node, 0)
	for _, n := range m.nodes {
		if skip != nil && skip(n) {
			continue
		}
		next, ok := transition(n.State, n.LastHeartbeatAt, now)
		if !ok || next == n.State {
			continue
		}
		n.State = next
		changed = append(changed, cloneNode(n))
	}
	return changed
}

func (m *Memory) SetRuntime(nodeID uint32, load, activeTransaction uint32) error {
	if nodeID == 0 {
		return fmt.Errorf("%w: node_id is required", ErrInvalidNode)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[nodeID]
	if !ok {
		return ErrNotFound
	}
	n.Load = load
	n.ActiveTransaction = activeTransaction
	return nil
}

var _ Registry = (*Memory)(nil)
