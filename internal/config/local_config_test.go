package config

import (
	"path/filepath"
	"testing"
	"time"
)

func TestParseMemoryBytes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want uint64
	}{
		{"8G", 8 << 30},
		{"8Gi", 8 << 30},
		{"512M", 512 << 20},
		{"", 0},
	}
	for _, tc := range cases {
		got, err := ParseMemoryBytes(tc.in)
		if err != nil {
			t.Fatalf("%q: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("%q: got %d want %d", tc.in, got, tc.want)
		}
	}
}

func TestLoadMasterLocal(t *testing.T) {
	t.Parallel()
	cfg, err := LoadMaster(filepath.Join("..", "..", "configs", "local", "master.dev.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Node.NodeID != 1 || cfg.HTTP.Port != 9200 || cfg.Node.Name != "master" {
		t.Fatalf("master = %+v", cfg)
	}
	if cfg.Resource.CPUCores != 1 || cfg.Resource.MemoryBytes() != 512<<20 {
		t.Fatalf("resource = %+v", cfg.Resource)
	}
}

func TestLoadLocalTopologyConfigs(t *testing.T) {
	t.Parallel()
	gw, err := LoadHTTP2GW(filepath.Join("..", "..", "configs", "local", "http2gw.dev.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if gw.Node.NodeID != 10 || gw.Resource.CPUCores != 4 || gw.Resource.MemoryBytes() != 8<<30 {
		t.Fatalf("http2gw local = %+v", gw)
	}
	if gw.MasterURL != "http://127.0.0.1:9200" {
		t.Fatalf("master_url = %s", gw.MasterURL)
	}

	l1, err := LoadLogic(filepath.Join("..", "..", "configs", "local", "logic-1.dev.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	l2, err := LoadLogic(filepath.Join("..", "..", "configs", "local", "logic-2.dev.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if l1.Node.NodeID != 2 || l1.UDP.Port != 9100 || l1.Resource.CPUCores != 6 {
		t.Fatalf("logic-1 = %+v", l1)
	}
	if l2.Node.NodeID != 3 || l2.UDP.Port != 9101 {
		t.Fatalf("logic-2 = %+v", l2)
	}

	cli, err := LoadLoadClient(filepath.Join("..", "..", "configs", "local", "http2client-1.dev.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cli.Node.NodeID != 20 || cli.QPS != 20 || cli.Concurrency != 4 {
		t.Fatalf("http2client-1 = %+v", cli)
	}
	if cli.Duration.Duration() != 30*time.Second {
		t.Fatalf("duration = %s", cli.Duration.Duration())
	}

	perf, err := LoadLoadClient(filepath.Join("..", "..", "configs", "local", "perf-1.dev.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if normalizeNodeType(perf.Node.Type) != "performance" || perf.Node.NodeID != 30 || perf.QPS != 50 {
		t.Fatalf("perf-1 = %+v", perf)
	}
}

func TestLoadClientRejectsWrongType(t *testing.T) {
	t.Parallel()
	cfg := LoadClient{
		Node: NodeMeta{NodeID: 1, InstanceID: "x", Type: "logic"},
		Target: "http://127.0.0.1:8080", MessageID: 1001, Concurrency: 1, N: 1,
		Timeout: Duration(time.Second),
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected type error")
	}
}
