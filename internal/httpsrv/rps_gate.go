package httpsrv

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/haison65/logic-gateway/internal/metrics"
	"golang.org/x/time/rate"
)

// rpsGate đếm request realtime, publish RPS mỗi 1s, và admission không block (Allow).
// Hot path chỉ atomic + Allow() — không Wait() để tránh tăng latency.
type rpsGate struct {
	received atomic.Uint64
	limited  atomic.Uint64
	rps      atomic.Uint64

	lim *rate.Limiter
	met *metrics.Metrics

	stop context.CancelFunc
}

func newRPSGate(maxRPS float64, burst int, met *metrics.Metrics) *rpsGate {
	g := &rpsGate{met: met}
	if maxRPS > 0 {
		if burst <= 0 {
			burst = int(maxRPS / 10)
			if burst < 1 {
				burst = 1
			}
			if burst > 2000 {
				burst = 2000
			}
		}
		g.lim = rate.NewLimiter(rate.Limit(maxRPS), burst)
	}
	return g
}

func (g *rpsGate) start(parent context.Context) {
	if g == nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	g.stop = cancel
	go g.loop(ctx)
}

func (g *rpsGate) close() {
	if g == nil || g.stop == nil {
		return
	}
	g.stop()
}

func (g *rpsGate) loop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	var prev uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cur := g.received.Load()
			delta := cur - prev
			prev = cur
			g.rps.Store(delta)
			if g.met != nil {
				g.met.SetHTTPRPS(float64(delta))
			}
		}
	}
}

// observeArrival tăng counter nhận được (mọi POST /v1/data vào handler).
func (g *rpsGate) observeArrival() {
	if g == nil {
		return
	}
	g.received.Add(1)
	if g.met != nil {
		g.met.AddHTTPReceived(1)
	}
}

// allow trả về false nếu vượt max_rps — không chờ token (reject ngay → 429).
func (g *rpsGate) allow() bool {
	if g == nil || g.lim == nil {
		return true
	}
	if g.lim.Allow() {
		return true
	}
	g.limited.Add(1)
	if g.met != nil {
		g.met.AddHTTPRateLimited(1)
	}
	return false
}

func (g *rpsGate) snapshot() (received, limited, rps uint64) {
	if g == nil {
		return 0, 0, 0
	}
	return g.received.Load(), g.limited.Load(), g.rps.Load()
}
