package router

import (
	"context"
	"strconv"
	"testing"

	"github.com/haison65/logic-gateway/internal/registry"
)

// Phase 5: Active-Active — round_robin trải đều 2 Logic ACTIVE.
func TestActiveActiveTwoLogicRoundRobin(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{
		node(2, registry.NodeStateActive, 1001),
		node(3, registry.NodeStateActive, 1001),
	}}
	rt := New(src, NewRoundRobin())
	hit := map[uint32]int{}
	for i := 0; i < 100; i++ {
		n, err := rt.Route(context.Background(), dataEnv(1001, "", uint64(i+1)))
		if err != nil {
			t.Fatal(err)
		}
		hit[n.ID]++
	}
	if hit[2] != 50 || hit[3] != 50 {
		t.Fatalf("expected 50/50, got %v", hit)
	}
}

// Phase 5: consistent_hash với đủ key đa dạng phải hit cả hai Logic.
func TestActiveActiveTwoLogicConsistentHash(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{
		node(2, registry.NodeStateActive, 1001),
		node(3, registry.NodeStateActive, 1001),
	}}
	rt := New(src, NewConsistentHash(64))
	hit := map[uint32]int{}
	const n = 300
	for i := 0; i < n; i++ {
		got, err := rt.Route(context.Background(), dataEnv(1001, "k-"+strconv.Itoa(i), 0))
		if err != nil {
			t.Fatal(err)
		}
		hit[got.ID]++
	}
	if hit[2] < n/20 || hit[3] < n/20 {
		t.Fatalf("expected both logics hit reasonably, got %v", hit)
	}
}

// Phase 5: Logic DEAD bị loại — traffic mới chỉ vào Logic còn ACTIVE.
func TestActiveActiveFailoverToRemainingLogic(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{
		node(2, registry.NodeStateDead, 1001),
		node(3, registry.NodeStateActive, 1001),
	}}
	rt := New(src, NewConsistentHash(64))
	for i := 0; i < 50; i++ {
		n, err := rt.Route(context.Background(), dataEnv(1001, "k-"+strconv.Itoa(i), 0))
		if err != nil {
			t.Fatal(err)
		}
		if n.ID != 3 {
			t.Fatalf("got %d want 3", n.ID)
		}
	}
}

// Phase 6: RouteExcluding bỏ node vừa fail.
func TestRouteExcludingSkipsFailedNode(t *testing.T) {
	t.Parallel()
	src := staticSource{nodes: []*registry.Node{
		node(2, registry.NodeStateActive, 1001),
		node(3, registry.NodeStateActive, 1001),
	}}
	rt := New(src, NewRoundRobin())
	n, err := rt.RouteExcluding(context.Background(), dataEnv(1001, "", 1), 2)
	if err != nil {
		t.Fatal(err)
	}
	if n.ID != 3 {
		t.Fatalf("got %d want 3", n.ID)
	}
}
