package logicnode

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/haison65/logic-gateway/internal/config"
	"go.uber.org/zap"
)

const (
	defaultDataWorkers  = 512
	defaultDataJobQueue = 4096
)

// dataWorkerPool: N worker cố định + bounded job queue.
// Enqueue không block receiveLoop — full thì drop (P0.2, Case A).
type dataWorkerPool struct {
	jobs    chan func()
	met     *nodeMetrics
	log     *zap.Logger
	busy    atomic.Int32
	closing atomic.Bool
	mu      sync.RWMutex
	wg      sync.WaitGroup
}

func newDataWorkerPool(n int, met *nodeMetrics, log *zap.Logger) *dataWorkerPool {
	if n <= 0 {
		n = defaultDataWorkers
	}
	queue := defaultDataJobQueue
	if met != nil {
		met.SetPoolCapacity(n)
		met.SetPoolInUse(0)
		met.SetJobQueueCapacity(queue)
		met.SetJobQueueDepth(0)
	}
	p := &dataWorkerPool{
		jobs: make(chan func(), queue),
		met:  met,
		log:  log,
	}
	for i := 0; i < n; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *dataWorkerPool) worker() {
	defer p.wg.Done()
	for fn := range p.jobs {
		if p.met != nil {
			p.met.SetJobQueueDepth(len(p.jobs))
		}
		n := p.busy.Add(1)
		if p.met != nil {
			p.met.SetPoolInUse(int(n))
		}
		fn()
		n = p.busy.Add(-1)
		if p.met != nil {
			p.met.SetPoolInUse(int(n))
			p.met.SetJobQueueDepth(len(p.jobs))
		}
	}
}

// Enqueue đưa job vào queue; không block. false = queue full.
func (p *dataWorkerPool) Enqueue(fn func()) bool {
	if p == nil {
		fn()
		return true
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closing.Load() {
		return false
	}
	select {
	case p.jobs <- fn:
		if p.met != nil {
			p.met.SetJobQueueDepth(len(p.jobs))
		}
		return true
	default:
		if p.met != nil {
			p.met.AddJobQueueDropped(1)
			if p.met.shouldLogPoolSaturation() && p.log != nil {
				p.log.Warn("logic data job queue full; dropping DATA_REQUEST (receiveLoop not blocked)",
					zap.Int("queue_depth", len(p.jobs)),
					zap.Int("queue_cap", cap(p.jobs)),
				)
			}
		}
		return false
	}
}

// Do: Enqueue không block; nếu full thì bỏ (đã metric) — không chạy sync trên receiveLoop.
func (p *dataWorkerPool) Do(ctx context.Context, fn func()) {
	if p == nil {
		fn()
		return
	}
	select {
	case <-ctx.Done():
		return
	default:
	}
	_ = p.Enqueue(fn)
}

func (p *dataWorkerPool) Close() {
	if p == nil || !p.closing.CompareAndSwap(false, true) {
		return
	}
	p.mu.Lock()
	close(p.jobs)
	p.mu.Unlock()
	p.wg.Wait()
}

func dataWorkerCount(cfg config.Logic) int {
	n := defaultDataWorkers
	if cfg.Resource.CPUCores > 0 {
		n = int(cfg.Resource.CPUCores) * 256
		if n < 128 {
			n = 128
		}
		if n > 2048 {
			n = 2048
		}
	}
	return n
}
