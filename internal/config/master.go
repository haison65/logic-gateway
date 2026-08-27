package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Master là cấu hình process Master (control-plane).
type Master struct {
	Node      NodeMeta  `yaml:"node"`
	HTTP      HTTP      `yaml:"http"`
	Heartbeat Heartbeat `yaml:"heartbeat"`
	Resource  Resource  `yaml:"resource"`
}

// NodeMeta identity dùng chung Master / LoadClient (và mở rộng YAML).
type NodeMeta struct {
	NodeID     uint32 `yaml:"node_id"`
	InstanceID string `yaml:"instance_id"`
	Name       string `yaml:"name"`
	Type       string `yaml:"type"` // master | http2_server | logic | http2_client | performance
	Group      string `yaml:"group"`
	IP         string `yaml:"ip"`
}

// LoadClient cấu hình HTTP2 Client hoặc Performance (cùng binary cmd/client).
type LoadClient struct {
	Node          NodeMeta `yaml:"node"`
	Target        string   `yaml:"target"`
	MessageID     uint32   `yaml:"message_id"`
	SessionID     string   `yaml:"session_id"`
	UniqueSession bool     `yaml:"unique_session"`
	Body          string   `yaml:"body"`
	Timeout       Duration `yaml:"timeout"`
	Concurrency   int      `yaml:"concurrency"`
	QPS           float64  `yaml:"qps"`
	Duration      Duration `yaml:"duration"`
	N             int      `yaml:"n"`
	MasterURL     string   `yaml:"master_url"`
	Resource      Resource `yaml:"resource"`
}

// LoadMaster đọc YAML Master.
func LoadMaster(path string) (Master, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Master{}, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Master
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Master{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return Master{}, err
	}
	return cfg, nil
}

func (c *Master) applyDefaults() {
	if c.Node.NodeID == 0 {
		c.Node.NodeID = 1
	}
	if strings.TrimSpace(c.Node.InstanceID) == "" {
		c.Node.InstanceID = "master-1"
	}
	if strings.TrimSpace(c.Node.Name) == "" {
		c.Node.Name = "master"
	}
	if strings.TrimSpace(c.Node.Type) == "" {
		c.Node.Type = "master"
	}
	if strings.TrimSpace(c.Node.Group) == "" {
		c.Node.Group = "local-server"
	}
	if strings.TrimSpace(c.HTTP.Listen) == "" {
		c.HTTP.Listen = "127.0.0.1"
	}
	if c.HTTP.Port == 0 {
		c.HTTP.Port = 9200
	}
	if c.Heartbeat.Interval.Duration() <= 0 {
		c.Heartbeat.Interval = Duration(time.Second)
	}
	if c.Heartbeat.Timeout.Duration() <= 0 {
		c.Heartbeat.Timeout = Duration(3 * time.Second)
	}
	if c.Heartbeat.DeadTimeout == 0 && c.Heartbeat.Timeout > 0 {
		c.Heartbeat.DeadTimeout = Duration(2 * c.Heartbeat.Timeout.Duration())
	}
}

func (c Master) Validate() error {
	if c.Node.NodeID == 0 {
		return fmt.Errorf("node.node_id must be > 0")
	}
	if normalizeNodeType(c.Node.Type) != "master" {
		return fmt.Errorf("node.type must be master, got %q", c.Node.Type)
	}
	if c.HTTP.Port <= 0 || c.HTTP.Port > 65535 {
		return fmt.Errorf("http.port %d out of range", c.HTTP.Port)
	}
	if c.Heartbeat.Interval.Duration() <= 0 {
		return fmt.Errorf("heartbeat.interval must be > 0")
	}
	if c.Heartbeat.Timeout.Duration() <= 0 {
		return fmt.Errorf("heartbeat.timeout must be > 0")
	}
	if c.Heartbeat.DeadTimeout.Duration() <= 0 {
		return fmt.Errorf("heartbeat.dead_timeout must be > 0")
	}
	if c.Heartbeat.Interval.Duration() >= c.Heartbeat.Timeout.Duration() {
		return fmt.Errorf("heartbeat.interval must be < heartbeat.timeout")
	}
	if c.Heartbeat.Timeout.Duration() >= c.Heartbeat.DeadTimeout.Duration() {
		return fmt.Errorf("heartbeat.timeout must be < heartbeat.dead_timeout")
	}
	return c.Resource.Validate("resource")
}

// LoadLoadClient đọc YAML http2client / performance.
func LoadLoadClient(path string) (LoadClient, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return LoadClient{}, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg LoadClient
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return LoadClient{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return LoadClient{}, err
	}
	return cfg, nil
}

func (c *LoadClient) applyDefaults() {
	if strings.TrimSpace(c.Node.Group) == "" {
		c.Node.Group = "tool-client"
	}
	if strings.TrimSpace(c.Target) == "" {
		c.Target = "http://127.0.0.1:8080"
	}
	if c.MessageID == 0 {
		c.MessageID = 1001
	}
	if strings.TrimSpace(c.SessionID) == "" {
		c.SessionID = c.Node.Name
		if c.SessionID == "" {
			c.SessionID = c.Node.InstanceID
		}
		if c.SessionID == "" {
			c.SessionID = "sess"
		}
	}
	if strings.TrimSpace(c.Body) == "" {
		c.Body = "hello"
	}
	if c.Timeout.Duration() <= 0 {
		c.Timeout = Duration(10 * time.Second)
	}
	if c.Concurrency < 1 {
		c.Concurrency = 1
	}
	if strings.TrimSpace(c.MasterURL) == "" {
		c.MasterURL = "http://127.0.0.1:9200"
	}
}

func (c LoadClient) Validate() error {
	if c.Node.NodeID == 0 {
		return fmt.Errorf("node.node_id must be > 0")
	}
	if strings.TrimSpace(c.Node.InstanceID) == "" {
		return fmt.Errorf("node.instance_id is required")
	}
	nt := normalizeNodeType(c.Node.Type)
	if nt != "http2_client" && nt != "performance" {
		return fmt.Errorf("node.type must be http2_client or performance, got %q", c.Node.Type)
	}
	if strings.TrimSpace(c.Target) == "" {
		return fmt.Errorf("target is required")
	}
	if c.MessageID == 0 {
		return fmt.Errorf("message_id must be > 0")
	}
	if c.Concurrency < 1 {
		return fmt.Errorf("concurrency must be >= 1")
	}
	if c.QPS < 0 {
		return fmt.Errorf("qps must be >= 0")
	}
	if c.N < 0 {
		return fmt.Errorf("n must be >= 0")
	}
	if c.N == 0 && c.Duration.Duration() <= 0 {
		return fmt.Errorf("need n > 0 or duration > 0")
	}
	if c.Timeout.Duration() <= 0 {
		return fmt.Errorf("timeout must be > 0")
	}
	return c.Resource.Validate("resource")
}

func normalizeNodeType(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "_")
	switch s {
	case "http2gw", "http2_server", "http2server":
		return "http2_server"
	case "http2_client", "http2client":
		return "http2_client"
	default:
		return s
	}
}
