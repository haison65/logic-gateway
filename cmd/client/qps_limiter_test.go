package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewQPSLimiterNilWhenZero(t *testing.T) {
	t.Parallel()
	if newQPSLimiter(0) != nil || newQPSLimiter(-1) != nil {
		t.Fatal("expected nil limiter")
	}
}

func TestQPSLimiterEvenStarts(t *testing.T) {
	t.Parallel()
	const qps = 50.0
	lim := newQPSLimiter(qps)
	if lim == nil {
		t.Fatal("nil limiter")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1100*time.Millisecond)
	defer cancel()

	var n atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if err := lim.Wait(ctx); err != nil {
				return
			}
			n.Add(1)
		}
	}()
	<-done
	got := n.Load()
	// ~50 trong 1.1s; cho phép lệch nhỏ do scheduling.
	if got < 45 || got > 60 {
		t.Fatalf("starts=%d, want ~50 (±)", got)
	}
}
