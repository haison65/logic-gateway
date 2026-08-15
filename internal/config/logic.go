package config

import (
	"fmt"
	"net"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Logic là cấu hình process Logic.
type Logic struct {
	Node      Node      `yaml:"node"`
	UDP       UDP       `yaml:"udp"`
	Gateway   Gateway   `yaml:"gateway"`
	Heartbeat Heartbeat `yaml:"heartbeat"`
	Services  []Service `yaml:"services"`
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
	if net.ParseIP(c.Gateway.Host) == nil {
		return fmt.Errorf("gateway.host %q is not a valid IP", c.Gateway.Host)
	}
	if c.Gateway.Port <= 0 || c.Gateway.Port > 65535 {
		return fmt.Errorf("gateway.port %d out of range", c.Gateway.Port)
	}
	if c.Heartbeat.Interval.Duration() <= 0 {
		return fmt.Errorf("heartbeat.interval must be > 0")
	}
	if ip := strings.TrimSpace(c.Node.IP); ip != "" && net.ParseIP(ip) == nil {
		return fmt.Errorf("node.ip %q is not a valid IP", c.Node.IP)
	}
	for i, svc := range c.Services {
		if svc.ServiceID == 0 {
			return fmt.Errorf("services[%d].service_id is required", i)
		}
	}
	return nil
}

// AdvertiseIP là địa chỉ Logic khai báo khi REGISTER (gateway dùng để gửi DATA).
func (c Logic) AdvertiseIP() string {
	if ip := strings.TrimSpace(c.Node.IP); ip != "" {
		return ip
	}
	host := strings.TrimSpace(c.UDP.Listen)
	if ip := net.ParseIP(host); ip != nil && !ip.IsUnspecified() {
		return host
	}
	return "127.0.0.1"
}
