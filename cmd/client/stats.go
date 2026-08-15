package main

import (
	"fmt"
	"sort"
	"time"
)

type stats struct {
	ok       int
	fail     int
	http2    int
	byStatus map[int]int
	lat      []time.Duration
}

func (s *stats) add(r callResult) {
	if s.byStatus == nil {
		s.byStatus = make(map[int]int)
	}
	s.lat = append(s.lat, r.latency)
	if r.protoMajor == 2 {
		s.http2++
	}
	if r.err != nil || r.status >= 400 || r.status == 0 {
		s.fail++
		if r.status != 0 {
			s.byStatus[r.status]++
		} else {
			s.byStatus[-1]++
		}
		return
	}
	s.ok++
	s.byStatus[r.status]++
}

func (s stats) print(elapsed time.Duration, c int) {
	total := s.ok + s.fail
	fmt.Printf("requests:     %d\n", total)
	fmt.Printf("concurrency:  %d\n", c)
	fmt.Printf("ok:           %d\n", s.ok)
	fmt.Printf("fail:         %d\n", s.fail)
	fmt.Printf("http2:        %d\n", s.http2)
	fmt.Printf("elapsed:      %s\n", elapsed.Round(time.Millisecond))
	if elapsed > 0 && total > 0 {
		fmt.Printf("rps:          %.1f\n", float64(total)/elapsed.Seconds())
	}
	if len(s.byStatus) > 0 {
		fmt.Print("status:       ")
		codes := make([]int, 0, len(s.byStatus))
		for code := range s.byStatus {
			codes = append(codes, code)
		}
		sort.Ints(codes)
		for i, code := range codes {
			if i > 0 {
				fmt.Print(" ")
			}
			if code < 0 {
				fmt.Printf("err=%d", s.byStatus[code])
			} else {
				fmt.Printf("%d=%d", code, s.byStatus[code])
			}
		}
		fmt.Println()
	}
	if len(s.lat) == 0 {
		return
	}
	cp := append([]time.Duration(nil), s.lat...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	sum := time.Duration(0)
	for _, d := range cp {
		sum += d
	}
	fmt.Printf("latency min:  %s\n", cp[0].Round(time.Microsecond))
	fmt.Printf("latency avg:  %s\n", (sum / time.Duration(len(cp))).Round(time.Microsecond))
	fmt.Printf("latency p50:  %s\n", percentile(cp, 50).Round(time.Microsecond))
	fmt.Printf("latency p95:  %s\n", percentile(cp, 95).Round(time.Microsecond))
	fmt.Printf("latency p99:  %s\n", percentile(cp, 99).Round(time.Microsecond))
	fmt.Printf("latency max:  %s\n", cp[len(cp)-1].Round(time.Microsecond))
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
