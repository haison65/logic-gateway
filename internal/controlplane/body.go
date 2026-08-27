package controlplane

import "github.com/haison65/logic-gateway/internal/config"

// BodyFromHTTP2GW tạo RegisterBody từ config gateway.
func BodyFromHTTP2GW(cfg config.HTTP2GW) RegisterBody {
	return RegisterBody{
		NodeID:      cfg.Node.NodeID,
		InstanceID:  cfg.Node.InstanceID,
		Name:        cfg.Node.Name,
		Type:        "http2_server",
		Group:       cfg.Node.Group,
		Address:     cfg.Node.IP,
		UDPPort:     uint32(cfg.UDP.Port),
		HTTPPort:    uint32(cfg.HTTP.Port),
		CPUCores:    cfg.Resource.CPUCores,
		Memory:      cfg.Resource.Memory,
		MemoryBytes: cfg.Resource.MemoryBytes(),
	}
}

// BodyFromLogic tạo RegisterBody từ config logic.
func BodyFromLogic(cfg config.Logic, udpPort uint32) RegisterBody {
	addr := cfg.AdvertiseHost()
	return RegisterBody{
		NodeID:      cfg.Node.NodeID,
		InstanceID:  cfg.Node.InstanceID,
		Name:        cfg.Node.Name,
		Type:        "logic",
		Group:       cfg.Node.Group,
		Address:     addr,
		UDPPort:     udpPort,
		CPUCores:    cfg.Resource.CPUCores,
		Memory:      cfg.Resource.Memory,
		MemoryBytes: cfg.Resource.MemoryBytes(),
	}
}

// BodyFromLoadClient tạo RegisterBody từ config client/perf.
func BodyFromLoadClient(cfg config.LoadClient) RegisterBody {
	return RegisterBody{
		NodeID:      cfg.Node.NodeID,
		InstanceID:  cfg.Node.InstanceID,
		Name:        cfg.Node.Name,
		Type:        cfg.Node.Type,
		Group:       cfg.Node.Group,
		Address:     cfg.Node.IP,
		CPUCores:    cfg.Resource.CPUCores,
		Memory:      cfg.Resource.Memory,
		MemoryBytes: cfg.Resource.MemoryBytes(),
	}
}
