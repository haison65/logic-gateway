package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// LoadSummary là JSON tóm tắt cuối run (hướng B).
type LoadSummary struct {
	Client      string             `json:"client"`
	StartedAt   string             `json:"started_at"`
	FinishedAt  string             `json:"finished_at"`
	Elapsed     string             `json:"elapsed"`
	ElapsedSec  float64            `json:"elapsed_sec"`
	Addr        string             `json:"addr"`
	Concurrency int                `json:"concurrency"`
	QPS         float64            `json:"qps"`
	Duration    string             `json:"duration,omitempty"`
	N           int                `json:"n"`
	CPUs        float64            `json:"cpus,omitempty"`
	Requests    int                `json:"requests"`
	OK          int                `json:"ok"`
	Fail        int                `json:"fail"`
	HTTP2       int                `json:"http2"`
	RPS         float64            `json:"rps"`
	SuccessRate float64            `json:"success_rate_pct"`
	ByStatus    map[string]int     `json:"by_status"`
	Latency     *LatencySummary    `json:"latency,omitempty"`
}

type LatencySummary struct {
	MinMS float64 `json:"min_ms"`
	AvgMS float64 `json:"avg_ms"`
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
	MaxMS float64 `json:"max_ms"`
}

func (s stats) buildSummary(opt options, started time.Time, elapsed time.Duration) LoadSummary {
	total := s.ok + s.fail
	var rps, success float64
	if elapsed > 0 && total > 0 {
		rps = float64(total) / elapsed.Seconds()
	}
	if total > 0 {
		success = 100 * float64(s.ok) / float64(total)
	}
	by := make(map[string]int, len(s.byStatus))
	for code, n := range s.byStatus {
		if code < 0 {
			by["err"] = n
		} else {
			by[strconv.Itoa(code)] = n
		}
	}
	sum := LoadSummary{
		Client:      opt.clientName,
		StartedAt:   started.Format(time.RFC3339Nano),
		FinishedAt:  time.Now().Format(time.RFC3339Nano),
		Elapsed:     elapsed.Round(time.Millisecond).String(),
		ElapsedSec:  elapsed.Seconds(),
		Addr:        opt.addr,
		Concurrency: opt.c,
		QPS:         opt.qps,
		N:           opt.n,
		CPUs:        opt.clientCPUs,
		Requests:    total,
		OK:          s.ok,
		Fail:        s.fail,
		HTTP2:       s.http2,
		RPS:         rps,
		SuccessRate: success,
		ByStatus:    by,
		Latency:     s.latencySummary(),
	}
	if opt.duration > 0 {
		sum.Duration = opt.duration.String()
	}
	if sum.Client == "" {
		sum.Client = "client"
	}
	return sum
}

func (s stats) latencySummary() *LatencySummary {
	if len(s.lat) == 0 {
		return nil
	}
	cp := append([]time.Duration(nil), s.lat...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	sum := time.Duration(0)
	for _, d := range cp {
		sum += d
	}
	avg := sum / time.Duration(len(cp))
	return &LatencySummary{
		MinMS: durationMS(cp[0]),
		AvgMS: durationMS(avg),
		P50MS: durationMS(percentile(cp, 50)),
		P95MS: durationMS(percentile(cp, 95)),
		P99MS: durationMS(percentile(cp, 99)),
		MaxMS: durationMS(cp[len(cp)-1]),
	}
}

func durationMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func writeSummaryJSON(path string, sum LoadSummary) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	raw, err := json.MarshalIndent(sum, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return fmt.Errorf("write summary %s: %w", path, err)
	}
	return nil
}
