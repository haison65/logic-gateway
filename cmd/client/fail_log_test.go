package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFailEntryJSON(t *testing.T) {
	t.Parallel()
	e := failEntryFromResult(7, callResult{
		status:    504,
		txID:      "99",
		traceID:   "cli-1-7",
		sessionID: "sess-7",
		latency:   5 * time.Second,
	})
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["transaction_id"] != "99" || m["trace_id"] != "cli-1-7" || int(m["status"].(float64)) != 504 {
		t.Fatalf("entry = %s", raw)
	}
}

func TestFailLoggerWritesJSONL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "fails.jsonl")
	fl, err := newFailLogger(path, 10)
	if err != nil || fl == nil {
		t.Fatalf("new: %v", err)
	}
	fl.log(failEntryFromResult(1, callResult{status: 504, txID: "1", traceID: "t1", latency: time.Millisecond}))
	fl.log(failEntryFromResult(2, callResult{status: 503, txID: "2", traceID: "t2", latency: time.Millisecond}))
	fl.close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if fl.written.Load() != 2 {
		t.Fatalf("written = %d", fl.written.Load())
	}
	if len(raw) == 0 || !containsAll(string(raw), `"transaction_id":"1"`, `"transaction_id":"2"`) {
		t.Fatalf("body = %s", raw)
	}
}

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		if !containsSubstring(s, p) {
			return false
		}
	}
	return true
}

func containsSubstring(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || findSub(s, sub))
}

func findSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
