package httpsrv

import (
	"context"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/metrics"
)

func TestRPSGateCountsAndPublishes(t *testing.T) {
	t.Parallel()
	met := metrics.New()
	g := newRPSGate(0, 0, met)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.start(ctx)
	defer g.close()

	for i := 0; i < 100; i++ {
		g.observeArrival()
		if !g.allow() {
			t.Fatal("unlimited gate rejected")
		}
	}
	recv, lim, _ := g.snapshot()
	if recv != 100 || lim != 0 {
		t.Fatalf("recv=%d lim=%d", recv, lim)
	}
	time.Sleep(1100 * time.Millisecond)
	if met.Snapshot().HTTPRPS < 1 {
		t.Fatalf("expected http_rps published, got %+v", met.Snapshot())
	}
}

func TestRPSGateRateLimitAllowNotWait(t *testing.T) {
	t.Parallel()
	met := metrics.New()
	g := newRPSGate(10, 1, met) // 10 rps, burst 1
	var rejected int
	for i := 0; i < 50; i++ {
		g.observeArrival()
		if !g.allow() {
			rejected++
		}
	}
	if rejected < 30 {
		t.Fatalf("expected many rejects with low max_rps, rejected=%d", rejected)
	}
	_, lim, _ := g.snapshot()
	if lim == 0 || met.Snapshot().HTTPRateLimitedTotal == 0 {
		t.Fatalf("limited=%d snap=%+v", lim, met.Snapshot())
	}
}
