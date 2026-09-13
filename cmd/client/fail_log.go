package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type failLogEntry struct {
	Time          string  `json:"time"`
	Seq           int     `json:"seq"`
	Status        int     `json:"status"`
	Err           string  `json:"err,omitempty"`
	TransactionID string  `json:"transaction_id"`
	TraceID       string  `json:"trace_id"`
	SessionID     string  `json:"session_id"`
	LatencyMS     float64 `json:"latency_ms"`
}

type failLogger struct {
	ch      chan failLogEntry
	done    chan struct{}
	written atomic.Int64
	dropped atomic.Int64
	max     int64
	path    string
}

func newFailLogger(path string, maxLines int) (*failLogger, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	if maxLines <= 0 {
		maxLines = 100_000
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open fail log %s: %w", path, err)
	}
	fl := &failLogger{
		ch:   make(chan failLogEntry, 4096),
		done: make(chan struct{}),
		max:  int64(maxLines),
		path: path,
	}
	go fl.loop(f)
	return fl, nil
}

func (f *failLogger) loop(file *os.File) {
	defer close(f.done)
	defer file.Close()
	w := bufio.NewWriterSize(file, 64*1024)
	defer w.Flush()
	enc := json.NewEncoder(w)
	for e := range f.ch {
		if f.max > 0 && f.written.Load() >= f.max {
			f.dropped.Add(1)
			continue
		}
		if err := enc.Encode(e); err != nil {
			continue
		}
		f.written.Add(1)
	}
}

func (f *failLogger) log(e failLogEntry) {
	if f == nil {
		return
	}
	select {
	case f.ch <- e:
	default:
		f.dropped.Add(1)
	}
}

func (f *failLogger) close() {
	if f == nil {
		return
	}
	close(f.ch)
	<-f.done
}

func (f *failLogger) summary() string {
	if f == nil {
		return ""
	}
	return fmt.Sprintf("fail_log: %s written=%d dropped=%d", f.path, f.written.Load(), f.dropped.Load())
}

func failEntryFromResult(seq int, r callResult) failLogEntry {
	errStr := ""
	if r.err != nil {
		errStr = r.err.Error()
	}
	return failLogEntry{
		Time:          time.Now().Format(time.RFC3339Nano),
		Seq:           seq,
		Status:        r.status,
		Err:           errStr,
		TransactionID: r.txID,
		TraceID:       r.traceID,
		SessionID:     r.sessionID,
		LatencyMS:     float64(r.latency) / float64(time.Millisecond),
	}
}
