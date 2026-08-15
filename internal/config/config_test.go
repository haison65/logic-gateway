package config

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLoadHTTP2GW(t *testing.T) {
	t.Parallel()
	cfg, err := LoadHTTP2GW(filepath.Join("..", "..", "configs", "http2gw.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Node.NodeID != 1 || cfg.HTTP.Port != 8080 || cfg.UDP.Port != 9000 {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.Heartbeat.DeadTimeout.Duration() != 6*time.Second {
		t.Fatalf("dead timeout = %s", cfg.Heartbeat.DeadTimeout.Duration())
	}
	if cfg.StrategyName() != "consistent_hash" {
		t.Fatalf("strategy = %s", cfg.StrategyName())
	}
}

func TestLoadLogic(t *testing.T) {
	t.Parallel()
	cfg, err := LoadLogic(filepath.Join("..", "..", "configs", "logic.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Node.NodeID != 2 || cfg.UDP.Port != 9100 || cfg.Gateway.Port != 9000 {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.AdvertiseIP() != "127.0.0.1" {
		t.Fatalf("advertise = %s", cfg.AdvertiseIP())
	}
	if len(cfg.Services) != 1 || cfg.Services[0].ServiceID != 100 {
		t.Fatalf("services = %+v", cfg.Services)
	}
}

func TestValidateRejectsBadStrategy(t *testing.T) {
	t.Parallel()
	cfg := HTTP2GW{
		Node:        Node{NodeID: 1},
		HTTP:        HTTP{Listen: "127.0.0.1", Port: 8080},
		UDP:         UDP{Listen: "127.0.0.1", Port: 9000},
		Heartbeat:   Heartbeat{Interval: Duration(time.Second), Timeout: Duration(3 * time.Second), DeadTimeout: Duration(6 * time.Second)},
		Transaction: Transaction{Timeout: Duration(5 * time.Second)},
		Routing:     Routing{Strategy: "random"},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error")
	}
}
