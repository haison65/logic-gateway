package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/haison65/logic-gateway/internal/netaddr"
	"gopkg.in/yaml.v3"
)

const (
	defaultHTTPListen = "0.0.0.0"
	defaultHTTPPort   = 8080
	defaultUDPListen  = "0.0.0.0"
	defaultNodeID     = 1
)

// HTTP2GW là cấu hình process gateway.
type HTTP2GW struct {
	Node        Node        `yaml:"node"`
	HTTP        HTTP        `yaml:"http"`
	UDP         UDP         `yaml:"udp"`
	Heartbeat   Heartbeat   `yaml:"heartbeat"`
	Transaction Transaction `yaml:"transaction"`
	Routing     Routing     `yaml:"routing"`
	// Services: capability + message_types HTTP2GW khai báo khi REGISTER (§6).
	Services []Service `yaml:"services"`
	Resource Resource  `yaml:"resource"`
	// MasterURL: control-plane (Phase 3+); trống = không báo cáo Master.
	MasterURL string `yaml:"master_url"`
}

type Node struct {
	NodeID     uint32 `yaml:"node_id"`
	InstanceID string `yaml:"instance_id"`
	Name       string `yaml:"name"`
	Type       string `yaml:"type"` // http2_server | logic (optional metadata)
	Group      string `yaml:"group"`
	IP         string `yaml:"ip"`
}

type HTTP struct {
	Listen  string `yaml:"listen"`
	Port    int    `yaml:"port"`
	TLSCert string `yaml:"tls_cert"`
	TLSKey  string `yaml:"tls_key"`
	// Remote là base URL HTTP của server ngoài cho chiều Logic → HTTP2GW → Remote (§12).
	// Design không ghi URL; để trống thì DATA_REQUEST từ Logic nhận Envelope ERROR NO_ROUTING_TARGET.
	Remote string `yaml:"remote"`
}

type UDP struct {
	Listen string `yaml:"listen"`
	Port   int    `yaml:"port"`
}

type Heartbeat struct {
	Interval    Duration `yaml:"interval"`
	Timeout     Duration `yaml:"timeout"`
	DeadTimeout Duration `yaml:"dead_timeout"`
}

type Transaction struct {
	Timeout Duration `yaml:"timeout"`
}

type Routing struct {
	Strategy string `yaml:"strategy"`
}

// LoadHTTP2GW đọc YAML rồi áp mặc định và kiểm tra.
func LoadHTTP2GW(path string) (HTTP2GW, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return HTTP2GW{}, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg HTTP2GW
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return HTTP2GW{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return HTTP2GW{}, err
	}
	return cfg, nil
}

func (c *HTTP2GW) applyDefaults() {
	if c.Node.NodeID == 0 {
		c.Node.NodeID = defaultNodeID
	}
	if strings.TrimSpace(c.Node.InstanceID) == "" {
		c.Node.InstanceID = "http2gw-1"
	}
	if strings.TrimSpace(c.HTTP.Listen) == "" {
		c.HTTP.Listen = defaultHTTPListen
	}
	if c.HTTP.Port == 0 {
		c.HTTP.Port = defaultHTTPPort
	}
	if strings.TrimSpace(c.UDP.Listen) == "" {
		c.UDP.Listen = defaultUDPListen
	}
	if c.Heartbeat.DeadTimeout == 0 && c.Heartbeat.Timeout > 0 {
		c.Heartbeat.DeadTimeout = Duration(2 * c.Heartbeat.Timeout.Duration())
	}
	if strings.TrimSpace(c.Routing.Strategy) == "" {
		c.Routing.Strategy = "consistent_hash"
	}
	if len(c.Services) == 0 {
		c.Services = []Service{{
			ServiceID:    1,
			ServiceName:  "http2gw",
			MessageTypes: []uint32{1001, 1002, 1003},
		}}
	}
	if strings.TrimSpace(c.Node.Name) == "" {
		c.Node.Name = c.Node.InstanceID
	}
	if strings.TrimSpace(c.Node.Type) == "" {
		c.Node.Type = "http2_server"
	}
	if strings.TrimSpace(c.Node.Group) == "" {
		c.Node.Group = "local-server"
	}
}

// Validate kiểm tra trường bắt buộc.
func (c HTTP2GW) Validate() error {
	if c.Node.NodeID == 0 {
		return fmt.Errorf("node.node_id must be > 0")
	}
	if ip := strings.TrimSpace(c.Node.IP); ip != "" {
		if _, err := netaddr.ParseHost(ip); err != nil {
			return fmt.Errorf("node.ip: %w", err)
		}
	}
	if c.HTTP.Port < 0 || c.HTTP.Port > 65535 {
		return fmt.Errorf("http.port %d out of range", c.HTTP.Port)
	}
	if (c.HTTP.TLSCert == "") != (c.HTTP.TLSKey == "") {
		return fmt.Errorf("http.tls_cert and http.tls_key must both be set or both empty")
	}
	if c.UDP.Port < 0 || c.UDP.Port > 65535 {
		return fmt.Errorf("udp.port %d out of range", c.UDP.Port)
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
	if c.Transaction.Timeout.Duration() <= 0 {
		return fmt.Errorf("transaction.timeout must be > 0")
	}
	switch strings.ToLower(strings.TrimSpace(c.Routing.Strategy)) {
	case "consistent_hash", "consistent-hash", "round_robin", "round-robin":
	default:
		return fmt.Errorf("routing.strategy %q is not supported", c.Routing.Strategy)
	}
	for i, svc := range c.Services {
		if svc.ServiceID == 0 {
			return fmt.Errorf("services[%d].service_id is required", i)
		}
	}
	return c.Resource.Validate("resource")
}

// StrategyName chuẩn hóa tên strategy.
func (c HTTP2GW) StrategyName() string {
	s := strings.ToLower(strings.TrimSpace(c.Routing.Strategy))
	s = strings.ReplaceAll(s, "-", "_")
	return s
}
