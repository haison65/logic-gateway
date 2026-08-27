package heartbeat

import (
	"fmt"
	"time"

	"github.com/haison65/logic-gateway/internal/registry"
)

// NextOnHeartbeat trả về trạng thái sau heartbeat hợp lệ.
// DEAD không được hồi sinh bằng heartbeat — phải Register lại.
func NextOnHeartbeat(from registry.NodeState) (registry.NodeState, error) {
	switch from {
	case registry.NodeStateInit, registry.NodeStateRegistered, registry.NodeStateActive, registry.NodeStateSuspect:
		return registry.NodeStateActive, nil
	case registry.NodeStateDead:
		return from, ErrDeadNode
	default:
		return from, fmt.Errorf("%w: from %s", ErrInvalidTransition, from)
	}
}

// NextOnTimeout trả về trạng thái sau khi so sánh elapsed với ngưỡng.
// changed = false nếu không cần đổi state.
func NextOnTimeout(from registry.NodeState, elapsed, suspectAfter, deadAfter time.Duration) (registry.NodeState, bool) {
	if from == registry.NodeStateDead || from == registry.NodeStateUnspecified || from == registry.NodeStateStopping {
		return from, false
	}
	if elapsed >= deadAfter {
		return registry.NodeStateDead, from != registry.NodeStateDead
	}
	if elapsed >= suspectAfter {
		return registry.NodeStateSuspect, from != registry.NodeStateSuspect
	}
	return from, false
}
