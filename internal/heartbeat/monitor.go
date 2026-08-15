package heartbeat

import (
	"context"
	"fmt"
	"time"

	"github.com/haison65/logic-gateway/internal/registry"
)

// Monitor định kỳ quét node và chuyển ACTIVE/REGISTERED → SUSPECT → DEAD.
type Monitor struct {
	registry  registry.Registry
	cfg       Config
	clock     Clock
	OnTimeout func(n int)
}

// NewMonitor tạo monitor. cfg phải hợp lệ.
func NewMonitor(reg registry.Registry, cfg Config, clock Clock) (*Monitor, error) {
	if reg == nil {
		return nil, fmt.Errorf("%w: registry is required", ErrInvalidConfig)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Monitor{
		registry: reg,
		cfg:      cfg,
		clock:    resolveClock(clock),
	}, nil
}

// Check chạy một vòng timeout (dùng trong test; không sleep).
func (m *Monitor) Check() []*registry.Node {
	now := m.clock.Now()
	changed := m.registry.ApplyTimeouts(now, func(state registry.NodeState, last time.Time, now time.Time) (registry.NodeState, bool) {
		elapsed := now.Sub(last)
		if elapsed < 0 {
			elapsed = 0
		}
		return NextOnTimeout(state, elapsed, m.cfg.SuspectTimeout, m.cfg.DeadTimeout)
	})
	if m.OnTimeout != nil && len(changed) > 0 {
		m.OnTimeout(len(changed))
	}
	return changed
}

// Run lặp theo Interval cho đến khi ctx bị hủy.
func (m *Monitor) Run(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("%w: monitor is nil", ErrInvalidConfig)
	}
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()

	m.Check()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			m.Check()
		}
	}
}
