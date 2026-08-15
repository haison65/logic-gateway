package main

import (
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	t.Parallel()
	in := []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}
	if got := percentile(in, 50); got != 3*time.Millisecond {
		t.Fatalf("p50 = %s", got)
	}
	if got := percentile(in, 99); got != 4*time.Millisecond {
		t.Fatalf("p99 = %s", got)
	}
}

func TestStatsAdd(t *testing.T) {
	t.Parallel()
	var s stats
	s.add(callResult{status: 200, protoMajor: 2, latency: time.Millisecond})
	s.add(callResult{status: 503, protoMajor: 2, latency: 2 * time.Millisecond})
	s.add(callResult{err: contextErr{}, latency: time.Millisecond})
	if s.ok != 1 || s.fail != 2 || s.http2 != 2 {
		t.Fatalf("stats = %+v", s)
	}
}

type contextErr struct{}

func (contextErr) Error() string { return "canceled" }
