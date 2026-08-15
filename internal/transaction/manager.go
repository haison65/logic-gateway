// Package transaction correlate DATA_REQUEST / DATA_RESPONSE theo transaction_id.
//
// Manager chỉ giữ trạng thái giao dịch. Không biết Router, UDP, Registry, Heartbeat.
package transaction

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

type result struct {
	env *pb.Envelope
	err error
}

type pending struct {
	id      uint64
	ch      chan result
	mu      sync.Mutex
	done    bool
	waiting bool
}

func (p *pending) claimWait() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.waiting {
		return false
	}
	p.waiting = true
	return true
}

func newPending(id uint64) *pending {
	return &pending{
		id: id,
		ch: make(chan result, 1),
	}
}

func (p *pending) tryDeliver(env *pb.Envelope, err error) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return false
	}
	p.done = true
	p.ch <- result{env: env, err: err}
	return true
}

// Manager sở hữu map transaction_id → waiter. An toàn khi dùng đồng thời.
type Manager struct {
	nextID atomic.Uint64

	mu     sync.Mutex
	closed bool
	wait   map[uint64]*pending
}

// NewManager tạo Manager rỗng.
func NewManager() *Manager {
	return &Manager{wait: make(map[uint64]*pending)}
}

// Create đăng ký waiter. id == 0 thì tự cấp ID. Phải gọi trước Transport.Send.
func (m *Manager) Create(id uint64) (uint64, error) {
	if m == nil {
		return 0, fmt.Errorf("%w: manager is nil", ErrInvalidTransaction)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrManagerClosed
	}
	if id == 0 {
		for {
			id = m.nextID.Add(1)
			if _, exists := m.wait[id]; !exists {
				break
			}
		}
	} else if _, exists := m.wait[id]; exists {
		return 0, fmt.Errorf("%w: %d", ErrTransactionExists, id)
	}

	m.wait[id] = newPending(id)
	return id, nil
}

// Wait chờ Complete, hủy ctx, hoặc Close. Complete-before-Wait vẫn nhận được response.
func (m *Manager) Wait(ctx context.Context, id uint64) (*pb.Envelope, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: manager is nil", ErrInvalidTransaction)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrManagerClosed
	}
	p, ok := m.wait[id]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnknownTransaction, id)
	}
	if !p.claimWait() {
		return nil, fmt.Errorf("%w: wait already in progress", ErrInvalidTransaction)
	}

	select {
	case r := <-p.ch:
		m.remove(id)
		return r.env, r.err
	case <-ctx.Done():
		_ = m.finish(id, nil, ctx.Err())
		r := <-p.ch
		m.remove(id)
		return r.env, r.err
	}
}

// Complete giao response. Lần Complete đầu thắng; lần sau ErrUnknownTransaction.
func (m *Manager) Complete(id uint64, env *pb.Envelope) error {
	return m.finish(id, env, nil)
}

// Cancel giải phóng waiter và xóa khỏi map.
func (m *Manager) Cancel(id uint64) error {
	err := m.finish(id, nil, context.Canceled)
	m.remove(id)
	return err
}

// Close idempotent; giải phóng mọi waiter bằng ErrManagerClosed.
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	waiters := make([]*pending, 0, len(m.wait))
	for _, p := range m.wait {
		waiters = append(waiters, p)
	}
	m.wait = make(map[uint64]*pending)
	m.mu.Unlock()

	for _, p := range waiters {
		p.tryDeliver(nil, ErrManagerClosed)
	}
	return nil
}

func (m *Manager) finish(id uint64, env *pb.Envelope, err error) error {
	if m == nil {
		return fmt.Errorf("%w: manager is nil", ErrInvalidTransaction)
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrManagerClosed
	}
	p, ok := m.wait[id]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %d", ErrUnknownTransaction, id)
	}
	if !p.tryDeliver(env, err) {
		return fmt.Errorf("%w: %d", ErrUnknownTransaction, id)
	}
	return nil
}

func (m *Manager) remove(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.wait, id)
}

func (m *Manager) has(id uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.wait[id]
	return ok
}
