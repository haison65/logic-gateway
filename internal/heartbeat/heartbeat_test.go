package heartbeat

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/registry"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{t: t}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func testCfg() Config {
	return Config{
		Interval:       10 * time.Millisecond,
		SuspectTimeout: 50 * time.Millisecond,
		DeadTimeout:    100 * time.Millisecond,
	}
}

func registerLogic(t *testing.T, r registry.Registry, id uint32) {
	t.Helper()
	err := r.Register(&registry.Node{
		ID:         id,
		InstanceID: "logic-1",
		Type:       registry.TypeLogic,
		Address:    "127.0.0.1",
		UDPPort:    9100,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	ok := testCfg()
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	bads := []Config{
		{Interval: 0, SuspectTimeout: time.Second, DeadTimeout: 2 * time.Second},
		{Interval: time.Second, SuspectTimeout: 0, DeadTimeout: 2 * time.Second},
		{Interval: time.Second, SuspectTimeout: time.Second, DeadTimeout: 2 * time.Second},
		{Interval: time.Millisecond, SuspectTimeout: 3 * time.Second, DeadTimeout: 3 * time.Second},
		{Interval: 2 * time.Second, SuspectTimeout: time.Second, DeadTimeout: 3 * time.Second},
	}
	for i, c := range bads {
		if err := c.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("case %d: err = %v", i, err)
		}
	}
}

func TestStateMachineHeartbeat(t *testing.T) {
	t.Parallel()

	cases := []struct {
		from registry.NodeState
		to   registry.NodeState
		err  error
	}{
		{registry.NodeStateRegistered, registry.NodeStateActive, nil},
		{registry.NodeStateActive, registry.NodeStateActive, nil},
		{registry.NodeStateSuspect, registry.NodeStateActive, nil},
		{registry.NodeStateDead, registry.NodeStateDead, ErrDeadNode},
		{registry.NodeStateUnspecified, registry.NodeStateUnspecified, ErrInvalidTransition},
	}
	for _, tc := range cases {
		got, err := NextOnHeartbeat(tc.from)
		if tc.err != nil {
			if !errors.Is(err, tc.err) || got != tc.to {
				t.Fatalf("from %s: got (%s, %v)", tc.from, got, err)
			}
			continue
		}
		if err != nil || got != tc.to {
			t.Fatalf("from %s: got (%s, %v) want %s", tc.from, got, err, tc.to)
		}
	}
}

func TestStateMachineTimeout(t *testing.T) {
	t.Parallel()

	suspect := 50 * time.Millisecond
	dead := 100 * time.Millisecond

	got, changed := NextOnTimeout(registry.NodeStateActive, 10*time.Millisecond, suspect, dead)
	if changed || got != registry.NodeStateActive {
		t.Fatalf("fresh active: %s changed=%v", got, changed)
	}

	got, changed = NextOnTimeout(registry.NodeStateActive, suspect, suspect, dead)
	if !changed || got != registry.NodeStateSuspect {
		t.Fatalf("active timeout: %s changed=%v", got, changed)
	}

	got, changed = NextOnTimeout(registry.NodeStateRegistered, suspect, suspect, dead)
	if !changed || got != registry.NodeStateSuspect {
		t.Fatalf("registered timeout: %s", got)
	}

	got, changed = NextOnTimeout(registry.NodeStateSuspect, suspect, suspect, dead)
	if changed || got != registry.NodeStateSuspect {
		t.Fatalf("suspect still within dead: %s changed=%v", got, changed)
	}

	got, changed = NextOnTimeout(registry.NodeStateSuspect, dead, suspect, dead)
	if !changed || got != registry.NodeStateDead {
		t.Fatalf("suspect → dead: %s changed=%v", got, changed)
	}

	got, changed = NextOnTimeout(registry.NodeStateActive, dead, suspect, dead)
	if !changed || got != registry.NodeStateDead {
		t.Fatalf("active long timeout → dead: %s", got)
	}

	got, changed = NextOnTimeout(registry.NodeStateDead, dead*2, suspect, dead)
	if changed || got != registry.NodeStateDead {
		t.Fatalf("dead stays dead: %s changed=%v", got, changed)
	}
}

func TestHandleRegisteredToActive(t *testing.T) {
	t.Parallel()

	reg := registry.NewMemory()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	registerLogic(t, reg, 7)
	svc := NewService(reg, clock)

	resp, err := svc.Handle(&pb.HeartbeatRequest{NodeId: 7, Sequence: 3})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if resp.GetSequence() != 3 || resp.GetTimestampMs() != uint64(clock.Now().UnixMilli()) {
		t.Fatalf("resp = %+v", resp)
	}
	n, _ := reg.Get(7)
	if n.State != registry.NodeStateActive {
		t.Fatalf("state = %s", n.State)
	}
}

func TestHandleActiveStaysActive(t *testing.T) {
	t.Parallel()

	reg := registry.NewMemory()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	registerLogic(t, reg, 1)
	svc := NewService(reg, clock)
	if _, err := svc.Handle(&pb.HeartbeatRequest{NodeId: 1, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Millisecond)
	if _, err := svc.Handle(&pb.HeartbeatRequest{NodeId: 1, Sequence: 2}); err != nil {
		t.Fatal(err)
	}
	n, _ := reg.Get(1)
	if n.State != registry.NodeStateActive {
		t.Fatalf("state = %s", n.State)
	}
}

func TestHandleStoresLoad(t *testing.T) {
	t.Parallel()
	reg := registry.NewMemory()
	registerLogic(t, reg, 1)
	svc := NewService(reg, nil)
	if _, err := svc.Handle(&pb.HeartbeatRequest{NodeId: 1, Sequence: 1, Load: 9, ActiveTransaction: 4}); err != nil {
		t.Fatal(err)
	}
	n, _ := reg.Get(1)
	if n.Load != 9 || n.ActiveTransaction != 4 {
		t.Fatalf("runtime %+v", n)
	}
}

func TestNextOnHeartbeatFromInit(t *testing.T) {
	t.Parallel()
	got, err := NextOnHeartbeat(registry.NodeStateInit)
	if err != nil || got != registry.NodeStateActive {
		t.Fatalf("got %s err %v", got, err)
	}
}

func TestHandleSuspectToActive(t *testing.T) {
	t.Parallel()

	reg := registry.NewMemory()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	registerLogic(t, reg, 1)
	svc := NewService(reg, clock)
	if _, err := svc.Handle(&pb.HeartbeatRequest{NodeId: 1}); err != nil {
		t.Fatal(err)
	}

	mon, err := NewMonitor(reg, testCfg(), clock)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(testCfg().SuspectTimeout)
	changed := mon.Check()
	if len(changed) != 1 || changed[0].State != registry.NodeStateSuspect {
		t.Fatalf("changed = %+v", changed)
	}

	if _, err := svc.Handle(&pb.HeartbeatRequest{NodeId: 1, Sequence: 9}); err != nil {
		t.Fatal(err)
	}
	n, _ := reg.Get(1)
	if n.State != registry.NodeStateActive {
		t.Fatalf("recovered state = %s", n.State)
	}
}

func TestHandleUnknownNode(t *testing.T) {
	t.Parallel()

	reg := registry.NewMemory()
	svc := NewService(reg, newFakeClock(time.Now()))
	_, err := svc.Handle(&pb.HeartbeatRequest{NodeId: 99, Sequence: 1})
	if !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("err = %v", err)
	}
	if len(reg.List()) != 0 {
		t.Fatal("unknown heartbeat must not register")
	}
}

func TestHandleInvalidRequest(t *testing.T) {
	t.Parallel()

	reg := registry.NewMemory()
	registerLogic(t, reg, 1)
	svc := NewService(reg, newFakeClock(time.Now()))
	_, err := svc.Handle(nil)
	if !errors.Is(err, ErrInvalidHeartbeat) {
		t.Fatalf("nil: %v", err)
	}
	_, err = svc.Handle(&pb.HeartbeatRequest{NodeId: 0})
	if !errors.Is(err, ErrInvalidHeartbeat) {
		t.Fatalf("zero id: %v", err)
	}
	n, _ := reg.Get(1)
	if n.State != registry.NodeStateRegistered {
		t.Fatalf("state mutated: %s", n.State)
	}
}

func TestHandleDeadRequiresReregister(t *testing.T) {
	t.Parallel()

	reg := registry.NewMemory()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	registerLogic(t, reg, 1)
	svc := NewService(reg, clock)
	if _, err := svc.Handle(&pb.HeartbeatRequest{NodeId: 1}); err != nil {
		t.Fatal(err)
	}
	mon, err := NewMonitor(reg, testCfg(), clock)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(testCfg().DeadTimeout)
	_ = mon.Check()
	n, _ := reg.Get(1)
	if n.State != registry.NodeStateDead {
		t.Fatalf("state = %s, want dead", n.State)
	}

	_, err = svc.Handle(&pb.HeartbeatRequest{NodeId: 1, Sequence: 1})
	if !errors.Is(err, ErrDeadNode) {
		t.Fatalf("heartbeat on dead: %v", err)
	}
	n, _ = reg.Get(1)
	if n.State != registry.NodeStateDead {
		t.Fatalf("dead revived by heartbeat: %s", n.State)
	}

	registerLogic(t, reg, 1)
	n, _ = reg.Get(1)
	if n.State != registry.NodeStateRegistered {
		t.Fatalf("re-register state = %s", n.State)
	}
	if _, err := svc.Handle(&pb.HeartbeatRequest{NodeId: 1}); err != nil {
		t.Fatal(err)
	}
	n, _ = reg.Get(1)
	if n.State != registry.NodeStateActive {
		t.Fatalf("after re-register heartbeat = %s", n.State)
	}
}

func TestMonitorTimeoutsAndRecovery(t *testing.T) {
	t.Parallel()

	reg := registry.NewMemory()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	registerLogic(t, reg, 1)
	svc := NewService(reg, clock)
	mon, err := NewMonitor(reg, testCfg(), clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Handle(&pb.HeartbeatRequest{NodeId: 1}); err != nil {
		t.Fatal(err)
	}

	clock.Advance(testCfg().SuspectTimeout)
	if got := mon.Check(); len(got) != 1 || got[0].State != registry.NodeStateSuspect {
		t.Fatalf("suspect: %+v", got)
	}

	clock.Advance(testCfg().DeadTimeout - testCfg().SuspectTimeout)
	if got := mon.Check(); len(got) != 1 || got[0].State != registry.NodeStateDead {
		t.Fatalf("dead: %+v", got)
	}
}

func TestMonitorRunStopsOnCancel(t *testing.T) {
	t.Parallel()

	reg := registry.NewMemory()
	mon, err := NewMonitor(reg, testCfg(), newFakeClock(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- mon.Run(ctx) }()
	time.Sleep(15 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("monitor did not stop")
	}
}
