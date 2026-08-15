package registry

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func sampleNode(id uint32, instance string) *Node {
	return &Node{
		ID:         id,
		InstanceID: instance,
		Type:       TypeLogic,
		Address:    "10.0.0.1",
		UDPPort:    9100,
		Services: []Service{{
			ID:           100,
			Name:         "call",
			MessageTypes: []uint32{1001, 1002},
		}},
	}
}

func TestRegisterAndGet(t *testing.T) {
	t.Parallel()

	r := NewMemory()
	n := sampleNode(100, "logic-1")
	if err := r.Register(n); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, ok := r.Get(100)
	if !ok {
		t.Fatal("expected node 100")
	}
	if got.ID != 100 || got.InstanceID != "logic-1" {
		t.Fatalf("got %+v", got)
	}
	if got.State != NodeStateRegistered {
		t.Fatalf("state = %v, want registered", got.State)
	}
	if got.LastHeartbeatAt.IsZero() {
		t.Fatal("LastHeartbeatAt should be set on register")
	}
	if got.UDPPort != 9100 || got.Address != "10.0.0.1" {
		t.Fatalf("address fields = %+v", got)
	}
	if len(got.Services) != 1 || got.Services[0].ID != 100 {
		t.Fatalf("services = %+v", got.Services)
	}
}

func TestGetUnknown(t *testing.T) {
	t.Parallel()

	r := NewMemory()
	if _, ok := r.Get(999); ok {
		t.Fatal("expected not found")
	}
}

func TestDuplicateRegisterUpdates(t *testing.T) {
	t.Parallel()

	r := NewMemory()
	if err := r.Register(sampleNode(100, "logic-1")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	updated := sampleNode(100, "logic-1-restarted")
	updated.Address = "10.0.0.9"
	updated.UDPPort = 9200
	if err := r.Register(updated); err != nil {
		t.Fatalf("re-register: %v", err)
	}

	list := r.List()
	if len(list) != 1 {
		t.Fatalf("len(List) = %d, want 1 (no duplicate)", len(list))
	}
	got, ok := r.Get(100)
	if !ok {
		t.Fatal("expected node 100 after update")
	}
	if got.InstanceID != "logic-1-restarted" || got.Address != "10.0.0.9" || got.UDPPort != 9200 {
		t.Fatalf("updated node = %+v", got)
	}
	if got.State != NodeStateRegistered {
		t.Fatalf("re-register should reset state to registered, got %s", got.State)
	}
	if got.LastHeartbeatAt.IsZero() {
		t.Fatal("re-register should set LastHeartbeatAt")
	}
}

func TestList(t *testing.T) {
	t.Parallel()

	r := NewMemory()
	if err := r.Register(sampleNode(1, "a")); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(sampleNode(2, "b")); err != nil {
		t.Fatal(err)
	}

	list := r.List()
	if len(list) != 2 {
		t.Fatalf("len(List) = %d, want 2", len(list))
	}
	seen := map[uint32]bool{}
	for _, n := range list {
		seen[n.ID] = true
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("list ids = %v", seen)
	}
}

func TestRemove(t *testing.T) {
	t.Parallel()

	r := NewMemory()
	if err := r.Register(sampleNode(7, "x")); err != nil {
		t.Fatal(err)
	}
	if err := r.Remove(7); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, ok := r.Get(7); ok {
		t.Fatal("node still present after Remove")
	}
}

func TestRemoveUnknown(t *testing.T) {
	t.Parallel()

	r := NewMemory()
	if err := r.Remove(1); err != ErrNotFound {
		t.Fatalf("Remove unknown = %v, want ErrNotFound", err)
	}
}

func TestRegisterInvalid(t *testing.T) {
	t.Parallel()

	r := NewMemory()
	if err := r.Register(nil); err == nil {
		t.Fatal("expected error for nil node")
	}
	if err := r.Register(&Node{ID: 0, InstanceID: "x"}); err == nil {
		t.Fatal("expected error for node_id 0")
	}
}

func TestReturnedNodeIsCopy(t *testing.T) {
	t.Parallel()

	r := NewMemory()
	if err := r.Register(sampleNode(5, "copy")); err != nil {
		t.Fatal(err)
	}

	got, ok := r.Get(5)
	if !ok {
		t.Fatal("expected node")
	}
	got.InstanceID = "mutated"
	got.Services[0].Name = "mutated"
	got.Services[0].MessageTypes[0] = 0

	again, _ := r.Get(5)
	if again.InstanceID != "copy" {
		t.Fatalf("internal InstanceID mutated: %s", again.InstanceID)
	}
	if again.Services[0].Name != "call" || again.Services[0].MessageTypes[0] != 1001 {
		t.Fatalf("internal Services mutated: %+v", again.Services)
	}

	list := r.List()
	list[0].Address = "0.0.0.0"
	again, _ = r.Get(5)
	if again.Address != "10.0.0.1" {
		t.Fatalf("List copy mutated internal Address: %s", again.Address)
	}
}

func TestConcurrentAccess(t *testing.T) {
	t.Parallel()

	r := NewMemory()
	const n = 64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id uint32) {
			defer wg.Done()
			node := sampleNode(id, "c")
			if err := r.Register(node); err != nil {
				t.Errorf("Register(%d): %v", id, err)
				return
			}
			if _, ok := r.Get(id); !ok {
				t.Errorf("Get(%d) after Register: missing", id)
			}
			_ = r.List()
			if id%2 == 0 {
				if err := r.Remove(id); err != nil {
					t.Errorf("Remove(%d): %v", id, err)
				}
			}
		}(uint32(i + 1))
	}
	wg.Wait()

	for id := uint32(1); id <= n; id++ {
		_, ok := r.Get(id)
		if id%2 == 0 && ok {
			t.Errorf("even id %d should have been removed", id)
		}
		if id%2 == 1 && !ok {
			t.Errorf("odd id %d should still exist", id)
		}
	}
	if got := len(r.List()); got != n/2 {
		t.Fatalf("len(List) = %d, want %d", got, n/2)
	}
}

func TestApplyHeartbeatAndTimeouts(t *testing.T) {
	t.Parallel()

	r := NewMemory()
	if err := r.Register(sampleNode(1, "hb")); err != nil {
		t.Fatal(err)
	}

	at := time.Now()
	got, err := r.ApplyHeartbeat(1, at, func(s NodeState) (NodeState, error) {
		if s != NodeStateRegistered {
			t.Fatalf("from = %s", s)
		}
		return NodeStateActive, nil
	})
	if err != nil {
		t.Fatalf("ApplyHeartbeat: %v", err)
	}
	if got.State != NodeStateActive || !got.LastHeartbeatAt.Equal(at) {
		t.Fatalf("got %+v", got)
	}

	_, err = r.ApplyHeartbeat(99, at, func(s NodeState) (NodeState, error) {
		return NodeStateActive, nil
	})
	if err != ErrNotFound {
		t.Fatalf("unknown heartbeat = %v", err)
	}

	changed := r.ApplyTimeouts(at.Add(10*time.Second), func(state NodeState, last, now time.Time) (NodeState, bool) {
		if state != NodeStateActive {
			return state, false
		}
		return NodeStateSuspect, true
	})
	if len(changed) != 1 || changed[0].State != NodeStateSuspect {
		t.Fatalf("changed = %+v", changed)
	}

	stored, _ := r.Get(1)
	if stored.State != NodeStateSuspect {
		t.Fatalf("stored state = %s", stored.State)
	}
}

func TestApplyHeartbeatRejectedLeavesState(t *testing.T) {
	t.Parallel()

	r := NewMemory()
	if err := r.Register(sampleNode(1, "dead")); err != nil {
		t.Fatal(err)
	}
	reject := errors.New("rejected")
	got, err := r.ApplyHeartbeat(1, time.Now(), func(NodeState) (NodeState, error) {
		return NodeStateUnspecified, reject
	})
	if !errors.Is(err, reject) {
		t.Fatalf("err = %v", err)
	}
	if got.State != NodeStateRegistered {
		t.Fatalf("state mutated to %s", got.State)
	}
	stored, _ := r.Get(1)
	if stored.State != NodeStateRegistered {
		t.Fatalf("internal state = %s", stored.State)
	}
}

func TestConcurrentHeartbeatAndTimeout(t *testing.T) {
	t.Parallel()

	r := NewMemory()
	const n = 32
	for id := uint32(1); id <= n; id++ {
		if err := r.Register(sampleNode(id, "c")); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(n * 2)
	for id := uint32(1); id <= n; id++ {
		go func(id uint32) {
			defer wg.Done()
			_, _ = r.ApplyHeartbeat(id, time.Now(), func(NodeState) (NodeState, error) {
				return NodeStateActive, nil
			})
		}(id)
		go func() {
			defer wg.Done()
			_ = r.ApplyTimeouts(time.Now().Add(time.Minute), func(state NodeState, last, now time.Time) (NodeState, bool) {
				if state == NodeStateDead {
					return state, false
				}
				return NodeStateSuspect, true
			})
		}()
	}
	wg.Wait()

	for id := uint32(1); id <= n; id++ {
		got, ok := r.Get(id)
		if !ok {
			t.Errorf("missing %d", id)
			continue
		}
		if got.State != NodeStateActive && got.State != NodeStateSuspect {
			t.Errorf("node %d state = %s", id, got.State)
		}
	}
}
