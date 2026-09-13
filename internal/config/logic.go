package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/haison65/logic-gateway/internal/netaddr"
	"gopkg.in/yaml.v3"
)

// Logic là cấu hình process Logic.
type Logic struct {
	Node      Node        `yaml:"node"`
	UDP       UDP         `yaml:"udp"`
	Gateway   Gateway     `yaml:"gateway"`
	Heartbeat Heartbeat   `yaml:"heartbeat"`
	Services  []Service   `yaml:"services"`
	Resource  Resource    `yaml:"resource"`
	MasterURL string      `yaml:"master_url"`
	// Metrics: HTTP Prometheus (/metrics). Port 0 = tắt.
	Metrics MetricsHTTP `yaml:"metrics"`
}

// MetricsHTTP là listen HTTP cho exposition Prometheus trên Logic.
type MetricsHTTP struct {
	Listen string `yaml:"listen"`
	Port   int    `yaml:"port"`
}

type Gateway struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type Service struct {
	ServiceID    uint32   `yaml:"service_id"`
	ServiceName  string   `yaml:"service_name"`
	MessageTypes []uint32 `yaml:"message_types"`
}

// LoadLogic đọc YAML Logic.
func LoadLogic(path string) (Logic, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Logic{}, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Logic
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Logic{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return Logic{}, err
	}
	return cfg, nil
}

func (c *Logic) applyDefaults() {
	if c.Node.NodeID == 0 {
		c.Node.NodeID = defaultNodeID
	}
	if strings.TrimSpace(c.Node.InstanceID) == "" {
		c.Node.InstanceID = "logic-1"
	}
	if strings.TrimSpace(c.UDP.Listen) == "" {
		c.UDP.Listen = defaultUDPListen
	}
	if strings.TrimSpace(c.Gateway.Host) == "" {
		c.Gateway.Host = "127.0.0.1"
	}
	if len(c.Services) == 0 {
		c.Services = []Service{{
			ServiceID:    100,
			ServiceName:  "call",
			MessageTypes: []uint32{1001, 1002, 1003},
		}}
	}
	if strings.TrimSpace(c.Node.Name) == "" {
		c.Node.Name = c.Node.InstanceID
	}
	if strings.TrimSpace(c.Node.Type) == "" {
		c.Node.Type = "logic"
	}
	if strings.TrimSpace(c.Node.Group) == "" {
		c.Node.Group = "local-server"
	}
	if strings.TrimSpace(c.Metrics.Listen) == "" && c.Metrics.Port > 0 {
		c.Metrics.Listen = "0.0.0.0"
	}
}

// Validate kiểm tra cấu hình Logic.
func (c Logic) Validate() error {
	if c.Node.NodeID == 0 {
		return fmt.Errorf("node.node_id must be > 0")
	}
	if strings.TrimSpace(c.Node.InstanceID) == "" {
		return fmt.Errorf("node.instance_id is required")
	}
	if c.UDP.Port < 0 || c.UDP.Port > 65535 {
		return fmt.Errorf("udp.port %d out of range", c.UDP.Port)
	}
	if _, err := netaddr.ParseHost(c.Gateway.Host); err != nil {
		return fmt.Errorf("gateway.host: %w", err)
	}
	if netaddr.IsUnspecified(c.Gateway.Host) {
		return fmt.Errorf("gateway.host %q is unspecified; use a reachable host or IP", c.Gateway.Host)
	}
	if c.Gateway.Port <= 0 || c.Gateway.Port > 65535 {
		return fmt.Errorf("gateway.port %d out of range", c.Gateway.Port)
	}
	if c.Heartbeat.Interval.Duration() <= 0 {
		return fmt.Errorf("heartbeat.interval must be > 0")
	}
	if ip := strings.TrimSpace(c.Node.IP); ip != "" {
		if _, err := netaddr.ParseHost(ip); err != nil {
			return fmt.Errorf("node.ip: %w", err)
		}
		if netaddr.IsUnspecified(ip) {
			return fmt.Errorf("node.ip %q is unspecified; advertise a reachable address", ip)
		}
	}
	for i, svc := range c.Services {
		if svc.ServiceID == 0 {
			return fmt.Errorf("services[%d].service_id is required", i)
		}
	}
	if c.Metrics.Port < 0 || c.Metrics.Port > 65535 {
		return fmt.Errorf("metrics.port %d out of range", c.Metrics.Port)
	}
	return c.Resource.Validate("resource")
}

// AdvertiseHost là địa chỉ Logic đưa vào REGISTER (sau khi resolve thành IP literal).
// Ưu tiên node.ip (IP hoặc hostname). Không dùng udp.listen khi listen là 0.0.0.0.
func (c Logic) AdvertiseHost() string {
	if ip := strings.TrimSpace(c.Node.IP); ip != "" {
		return ip
	}
	host := strings.TrimSpace(c.UDP.Listen)
	if host != "" && !netaddr.IsUnspecified(host) {
		if _, err := netaddr.ParseHost(host); err == nil {
			return host
		}
	}
	return "127.0.0.1"
}

// AdvertiseIP giữ tên cũ: cùng giá trị AdvertiseHost (có thể là hostname trước resolve).
func (c Logic) AdvertiseIP() string {
	return c.AdvertiseHost()
}
