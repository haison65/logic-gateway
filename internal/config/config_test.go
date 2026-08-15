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

func TestLoadHTTP2GWDevYAMLUnchanged(t *testing.T) {
	t.Parallel()
	cfg, err := LoadHTTP2GW(filepath.Join("..", "..", "configs", "http2gw.dev.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.Listen != "127.0.0.1" || cfg.UDP.Listen != "127.0.0.1" || cfg.HTTP.Port != 8080 {
		t.Fatalf("dev yaml changed: %+v", cfg)
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
	if cfg.AdvertiseIP() != "127.0.0.1" || cfg.AdvertiseHost() != "127.0.0.1" {
		t.Fatalf("advertise = %s", cfg.AdvertiseIP())
	}
	if len(cfg.Services) != 1 || cfg.Services[0].ServiceID != 100 {
		t.Fatalf("services = %+v", cfg.Services)
	}
}

func TestLoadDockerYAML(t *testing.T) {
	t.Parallel()
	gw, err := LoadHTTP2GW(filepath.Join("..", "..", "configs", "http2gw.docker.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if gw.HTTP.Listen != "0.0.0.0" || gw.UDP.Listen != "0.0.0.0" {
		t.Fatalf("http2gw docker listen = %s / %s", gw.HTTP.Listen, gw.UDP.Listen)
	}
	lg, err := LoadLogic(filepath.Join("..", "..", "configs", "logic.docker.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if lg.UDP.Listen != "0.0.0.0" || lg.Gateway.Host != "http2gw" || lg.Node.IP != "logic" {
		t.Fatalf("logic docker cfg = %+v", lg)
	}
}

func TestLoadLogicDevYAMLUnchanged(t *testing.T) {
	t.Parallel()
	cfg, err := LoadLogic(filepath.Join("..", "..", "configs", "logic.dev.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.UDP.Listen != "127.0.0.1" || cfg.Gateway.Host != "127.0.0.1" || cfg.Node.IP != "127.0.0.1" {
		t.Fatalf("dev yaml changed: %+v", cfg)
	}
}

func TestLogicGatewayHostAllowsHostname(t *testing.T) {
	t.Parallel()
	cfg := validLogic()
	cfg.Gateway.Host = "http2gw"
	cfg.Node.IP = "logic"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.AdvertiseHost() != "logic" {
		t.Fatalf("advertise = %s", cfg.AdvertiseHost())
	}
}

func TestLogicGatewayHostRejectsUnspecifiedAndJunk(t *testing.T) {
	t.Parallel()
	cfg := validLogic()
	cfg.Gateway.Host = "0.0.0.0"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unspecified gateway.host error")
	}
	cfg = validLogic()
	cfg.Gateway.Host = "http2gw:9000"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected host:port error")
	}
	cfg = validLogic()
	cfg.Node.IP = "0.0.0.0"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unspecified node.ip error")
	}
}

func validLogic() Logic {
	return Logic{
		Node:      Node{NodeID: 2, InstanceID: "logic-1", IP: "127.0.0.1"},
		UDP:       UDP{Listen: "127.0.0.1", Port: 9100},
		Gateway:   Gateway{Host: "127.0.0.1", Port: 9000},
		Heartbeat: Heartbeat{Interval: Duration(time.Second)},
		Services:  []Service{{ServiceID: 100, ServiceName: "call", MessageTypes: []uint32{1001}}},
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
