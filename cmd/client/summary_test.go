package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteSummaryJSON(t *testing.T) {
	t.Parallel()
	var st stats
	st.add(callResult{status: 200, protoMajor: 2, latency: time.Millisecond})
	st.add(callResult{status: 504, protoMajor: 2, latency: 5 * time.Second})

	dir := t.TempDir()
	path := filepath.Join(dir, "client-1.summary.json")
	opt := options{clientName: "client-1", c: 10, qps: 0, addr: "http://http2gw:8080/v1/data", clientCPUs: 2}
	sum := st.buildSummary(opt, time.Now().Add(-30*time.Second), 30*time.Second)
	if err := writeSummaryJSON(path, sum); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got LoadSummary
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Client != "client-1" || got.Requests != 2 || got.OK != 1 || got.Fail != 1 {
		t.Fatalf("summary = %+v", got)
	}
	if got.ByStatus["200"] != 1 || got.ByStatus["504"] != 1 {
		t.Fatalf("by_status = %+v", got.ByStatus)
	}
	if got.Latency == nil || got.Latency.P50MS <= 0 {
		t.Fatalf("latency = %+v", got.Latency)
	}
	if got.SuccessRate < 49 || got.SuccessRate > 51 {
		t.Fatalf("success_rate = %v", got.SuccessRate)
	}
}
