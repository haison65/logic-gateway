package logicnode

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/config"
)

func TestDataWorkerPoolBounds(t *testing.T) {
	t.Parallel()
	p := newDataWorkerPool(2, nil, nil)
	t.Cleanup(p.Close)
	var running atomic.Int32
	var peak atomic.Int32
	done := make(chan struct{})

	const n = 20
	var left atomic.Int32
	left.Store(n)
	for i := 0; i < n; i++ {
		ok := p.Enqueue(func() {
			cur := running.Add(1)
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			running.Add(-1)
			if left.Add(-1) == 0 {
				close(done)
			}
		})
		if !ok {
			t.Fatal("enqueue failed unexpectedly")
		}
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}
	if peak.Load() > 2 {
		t.Fatalf("peak concurrency = %d, want <= 2", peak.Load())
	}
}

func TestDataWorkerPoolEnqueueDoesNotBlockWhenFull(t *testing.T) {
	t.Parallel()
	p := newDataWorkerPool(1, nil, nil)
	block := make(chan struct{})
	started := make(chan struct{})
	t.Cleanup(func() {
		close(block)
		p.Close()
	})

	if !p.Enqueue(func() {
		close(started)
		<-block
	}) {
		t.Fatal("first enqueue failed")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}

	for i := 0; i < defaultDataJobQueue; i++ {
		if !p.Enqueue(func() {}) {
			t.Fatalf("unexpected drop while filling at %d", i)
		}
	}
	start := time.Now()
	if p.Enqueue(func() {}) {
		t.Fatal("expected drop when queue full")
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatalf("Enqueue blocked for %s", time.Since(start))
	}
}

func TestDataWorkerCountFromCPU(t *testing.T) {
	t.Parallel()
	got := dataWorkerCount(config.Logic{Resource: config.Resource{CPUCores: 2}})
	if got != 512 {
		t.Fatalf("workers = %d, want 512", got)
	}
}

func TestDataWorkerPoolConcurrentCloseAndEnqueue(t *testing.T) {
	p := newDataWorkerPool(2, nil, nil)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			p.Enqueue(func() {})
		}
	}()

	p.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("enqueue did not finish after close")
	}
}
