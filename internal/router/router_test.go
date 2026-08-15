package router

import (
	"context"
	"errors"
	"hash/fnv"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/registry"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

type staticSource struct {
	nodes []*registry.Node
}

func (s staticSource) List() []*registry.Node {
	out := make([]*registry.Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		copied := *n
		copied.Services = append([]registry.Service(nil), n.Services...)
		for i := range copied.Services {
			copied.Services[i].MessageTypes = append([]uint32(nil), n.Services[i].MessageTypes...)
		}
		out = append(out, &copied)
	}
	return out
}

func node(id uint32, state registry.NodeState, types ...uint32) *registry.Node {
	return &registry.Node{
		ID:       id,
		Type:     registry.TypeLogic,
		Address:  "10.0.0." + strconv.FormatUint(uint64(id), 10),
		UDPPort:  9000 + id,
		State:    state,
		Services: []registry.Service{{ID: 100, Name: "call", MessageTypes: types}},
	}
}

func dataEnv(msgID uint32, session string, tx uint64) *pb.Envelope {
	return &pb.Envelope{
		Version:       1,
		Type:          pb.MessageType_MESSAGE_TYPE_DATA_REQUEST,
		TransactionId: tx,
		Body: &pb.Envelope_DataRequest{
			DataRequest: &pb.DataRequest{MessageId: msgID, SessionId: session, Payload: []byte("x")},
		},
	}
}

func TestRouteOnlyActiveNodes(t *testing.T) {
	t.Parallel()

	src := staticSource{nodes: []*registry.Node{
		node(1, registry.NodeStateActive, 1001),
		node(2, registry.NodeStateSuspect, 1001),
		node(3, registry.NodeStateDead, 1001),
	}}
	got, err := New(src, NewRoundRobin()).Route(context.Background(), dataEnv(1001, "", 1))
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 1 {
		t.Fatalf("got %d, want 1", got.ID)
	}
}

func TestRegisteredNodeNotSelected(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{node(1, registry.NodeStateRegistered, 1001)}}
	_, err := New(src, NewRoundRobin()).Route(context.Background(), dataEnv(1001, "", 1))
	if !errors.Is(err, ErrNoEligibleNode) {
		t.Fatalf("err = %v", err)
	}
}

func TestSuspectNodeNotSelected(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{node(1, registry.NodeStateSuspect, 1001)}}
	_, err := New(src, NewRoundRobin()).Route(context.Background(), dataEnv(1001, "", 1))
	if !errors.Is(err, ErrNoEligibleNode) {
		t.Fatalf("err = %v", err)
	}
}

func TestDeadNodeNotSelected(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{node(1, registry.NodeStateDead, 1001)}}
	_, err := New(src, NewRoundRobin()).Route(context.Background(), dataEnv(1001, "", 1))
	if !errors.Is(err, ErrNoEligibleNode) {
		t.Fatalf("err = %v", err)
	}
}

func TestRouteByMessageType(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{
		node(1, registry.NodeStateActive, 1001),
		node(2, registry.NodeStateActive, 2001),
	}}
	got, err := New(src, NewRoundRobin()).Route(context.Background(), dataEnv(2001, "", 1))
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 2 {
		t.Fatalf("got %d, want 2", got.ID)
	}
}

func TestNoEligibleNode(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{node(1, registry.NodeStateActive, 1001)}}
	_, err := New(src, NewRoundRobin()).Route(context.Background(), dataEnv(9999, "", 1))
	if !errors.Is(err, ErrNoEligibleNode) {
		t.Fatalf("err = %v", err)
	}
}

func TestUnsupportedEnvelopeType(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{node(1, registry.NodeStateActive, 1001)}}
	env := &pb.Envelope{Type: pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST}
	_, err := New(src, NewRoundRobin()).Route(context.Background(), env)
	if !errors.Is(err, ErrUnsupportedMessageType) {
		t.Fatalf("err = %v", err)
	}
}

func TestRoundRobin(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{
		node(3, registry.NodeStateActive, 1001),
		node(1, registry.NodeStateActive, 1001),
		node(2, registry.NodeStateActive, 1001),
	}}
	rt := New(src, NewRoundRobin())
	want := []uint32{1, 2, 3, 1, 2, 3}
	for i, id := range want {
		got, err := rt.Route(context.Background(), dataEnv(1001, "", uint64(i+1)))
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != id {
			t.Fatalf("i=%d got %d want %d", i, got.ID, id)
		}
	}
}

func TestRoundRobinSkipsInactiveNodes(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{
		node(1, registry.NodeStateActive, 1001),
		node(2, registry.NodeStateSuspect, 1001),
		node(3, registry.NodeStateActive, 1001),
	}}
	rt := New(src, NewRoundRobin())
	want := []uint32{1, 3, 1, 3}
	for i, id := range want {
		got, err := rt.Route(context.Background(), dataEnv(1001, "", uint64(i+1)))
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != id {
			t.Fatalf("i=%d got %d want %d", i, got.ID, id)
		}
	}
}

func TestRoundRobinConcurrent(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{
		node(1, registry.NodeStateActive, 1001),
		node(2, registry.NodeStateActive, 1001),
	}}
	rt := New(src, NewRoundRobin())
	const n = 200
	ids := make(chan uint32, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			got, err := rt.Route(context.Background(), dataEnv(1001, "", 1))
			if err != nil {
				t.Errorf("Route: %v", err)
				return
			}
			if got.ID != 1 && got.ID != 2 {
				t.Errorf("unexpected id %d", got.ID)
				return
			}
			ids <- got.ID
		}()
	}
	wg.Wait()
	close(ids)
	counts := map[uint32]int{}
	for id := range ids {
		counts[id]++
	}
	if counts[1] == 0 || counts[2] == 0 {
		t.Fatalf("distribution = %v", counts)
	}
}

func TestConsistentHashDeterministic(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{
		node(1, registry.NodeStateActive, 1001),
		node(2, registry.NodeStateActive, 1001),
		node(3, registry.NodeStateActive, 1001),
	}}
	rt := New(src, NewConsistentHash(32))
	env := dataEnv(1001, "request-123", 0)
	first, err := rt.Route(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		got, err := rt.Route(context.Background(), env)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != first.ID {
			t.Fatalf("iteration %d: %d != %d", i, got.ID, first.ID)
		}
	}
}

func TestConsistentHashDistribution(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{
		node(1, registry.NodeStateActive, 1001),
		node(2, registry.NodeStateActive, 1001),
		node(3, registry.NodeStateActive, 1001),
	}}
	rt := New(src, NewConsistentHash(64))
	counts := map[uint32]int{}
	const n = 300
	for i := 0; i < n; i++ {
		got, err := rt.Route(context.Background(), dataEnv(1001, "k-"+strconv.Itoa(i), 0))
		if err != nil {
			t.Fatal(err)
		}
		counts[got.ID]++
	}
	for id := uint32(1); id <= 3; id++ {
		if counts[id] < n/20 {
			t.Fatalf("node %d too few: %v", id, counts)
		}
	}
}

func TestConsistentHashMinimalRemapping(t *testing.T) {
	t.Parallel()

	base := []*registry.Node{
		node(1, registry.NodeStateActive, 1001),
		node(2, registry.NodeStateActive, 1001),
		node(3, registry.NodeStateActive, 1001),
	}
	plus := append(append([]*registry.Node(nil), base...), node(4, registry.NodeStateActive, 1001))
	ch := NewConsistentHash(64)

	const n = 1000
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "req-" + strconv.Itoa(i)
	}

	before := map[string]uint32{}
	for _, k := range keys {
		got, err := ch.Select(base, k)
		if err != nil {
			t.Fatal(err)
		}
		before[k] = got.ID
	}
	movedCH := 0
	for _, k := range keys {
		got, err := ch.Select(plus, k)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != before[k] {
			movedCH++
		}
	}

	movedMod := 0
	for _, k := range keys {
		if naiveModulo(k, []uint32{1, 2, 3}) != naiveModulo(k, []uint32{1, 2, 3, 4}) {
			movedMod++
		}
	}
	if movedCH >= movedMod {
		t.Fatalf("consistent hash moved %d keys, modulo moved %d", movedCH, movedMod)
	}
}

func naiveModulo(key string, ids []uint32) uint32 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return ids[h.Sum64()%uint64(len(ids))]
}

func TestRouterDoesNotMutateSource(t *testing.T) {
	t.Parallel()
	original := node(1, registry.NodeStateActive, 1001)
	src := staticSource{nodes: []*registry.Node{original}}
	got, err := New(src, NewRoundRobin()).Route(context.Background(), dataEnv(1001, "", 1))
	if err != nil {
		t.Fatal(err)
	}
	got.State = registry.NodeStateDead
	got.Services[0].MessageTypes[0] = 0
	got.Address = "mutated"
	if original.State != registry.NodeStateActive || original.Services[0].MessageTypes[0] != 1001 || original.Address != "10.0.0.1" {
		t.Fatalf("source mutated: %+v", original)
	}
}

func TestIntegrationRegistryAndRouter(t *testing.T) {
	t.Parallel()

	reg := registry.NewMemory()
	for _, n := range []*registry.Node{
		node(1, registry.NodeStateRegistered, 1001),
		node(2, registry.NodeStateRegistered, 1001, 2001),
		node(3, registry.NodeStateRegistered, 1001),
	} {
		if err := reg.Register(n); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now()
	toActive := func(registry.NodeState) (registry.NodeState, error) {
		return registry.NodeStateActive, nil
	}
	if _, err := reg.ApplyHeartbeat(1, now, toActive); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.ApplyHeartbeat(2, now, toActive); err != nil {
		t.Fatal(err)
	}

	changed := reg.ApplyTimeouts(now.Add(time.Hour), func(state registry.NodeState, last, at time.Time) (registry.NodeState, bool) {
		if state == registry.NodeStateRegistered {
			return registry.NodeStateSuspect, true
		}
		return state, false
	})
	if len(changed) != 1 || changed[0].ID != 3 {
		t.Fatalf("expected node 3 suspect, got %+v", changed)
	}

	rt := New(reg, NewRoundRobin())
	got, err := rt.Route(context.Background(), dataEnv(2001, "sess-a", 9))
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 2 || got.Address == "" || got.UDPPort == 0 {
		t.Fatalf("want node 2 with addr, got %+v", got)
	}

	got, err = rt.Route(context.Background(), dataEnv(1001, "", 10))
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 1 && got.ID != 2 {
		t.Fatalf("unexpected node %d", got.ID)
	}

	n3, ok := reg.Get(3)
	if !ok || n3.State != registry.NodeStateSuspect {
		t.Fatalf("router mutated node 3: %+v", n3)
	}
	n1, _ := reg.Get(1)
	if n1.State != registry.NodeStateActive {
		t.Fatalf("router mutated node 1: %s", n1.State)
	}
}
