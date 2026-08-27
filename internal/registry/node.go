package registry

import (
	"fmt"
	"time"
)

// NodeState là vòng đời node ở tầng domain.
type NodeState int

const (
	NodeStateUnspecified NodeState = iota
	NodeStateInit
	NodeStateRegistered
	NodeStateActive
	NodeStateSuspect
	NodeStateDead
	NodeStateStopping // graceful shutdown (control-plane; chưa dùng bởi Monitor DATA)
)

func (s NodeState) String() string {
	switch s {
	case NodeStateInit:
		return "init"
	case NodeStateRegistered:
		return "registered"
	case NodeStateActive:
		return "active"
	case NodeStateSuspect:
		return "suspect"
	case NodeStateDead:
		return "dead"
	case NodeStateStopping:
		return "stopping"
	default:
		return "unspecified"
	}
}

// Type là vai trò node ở tầng domain (khớp proto NodeType ints).
type Type uint32

const (
	TypeUnspecified Type = 0
	TypeHTTP2GW     Type = 1 // HTTP2_SERVER
	TypeLogic       Type = 2
	TypeMaster      Type = 3
	TypeHTTP2Client Type = 4
	TypePerformance Type = 5
)

func (t Type) String() string {
	switch t {
	case TypeHTTP2GW:
		return "http2_server"
	case TypeLogic:
		return "logic"
	case TypeMaster:
		return "master"
	case TypeHTTP2Client:
		return "http2_client"
	case TypePerformance:
		return "performance"
	default:
		return "unspecified"
	}
}

// IsDataPlaneType: được phép REGISTER qua UDP vào HTTP2GW (routing).
func (t Type) IsDataPlaneType() bool {
	return t == TypeHTTP2GW || t == TypeLogic
}

// IsLogicBackend: ứng viên router DATA.
func (t Type) IsLogicBackend() bool {
	return t == TypeLogic
}

// Service mô tả một năng lực dịch vụ mà node quảng bá.
type Service struct {
	ID           uint32
	Name         string
	MessageTypes []uint32
}

// ResourceInfo là metadata tài nguyên (application config + runtime snapshot).
// Limit thật (cgroup/Docker) nằm ở deployment layer — không suy ra từ field này.
type ResourceInfo struct {
	CPULimitCores    float64 // cấu hình (vd 4, 6)
	MemoryLimitBytes uint64  // cấu hình (vd 8GiB)
	CPUUsageCores    float64 // runtime (optional, 0 = chưa đo)
	MemoryUsageBytes uint64  // runtime (optional)
}

// Node là trạng thái đăng ký trong Registry, không phải message wire.
type Node struct {
	ID                uint32
	InstanceID        string
	Name              string // hiển thị: logic-1, perf-1, …
	Type              Type
	Group             string // vd local-server, tool-client
	Address           string
	UDPPort           uint32
	HTTPPort          uint32 // control/health nếu có
	Services          []Service
	State             NodeState
	LastHeartbeatAt   time.Time
	Load              uint32
	ActiveTransaction uint32
	Resource          ResourceInfo
}

func cloneNode(n *Node) *Node {
	if n == nil {
		return nil
	}
	out := *n
	out.Services = cloneServices(n.Services)
	return &out
}

func cloneServices(in []Service) []Service {
	if in == nil {
		return nil
	}
	out := make([]Service, len(in))
	for i, s := range in {
		out[i] = s
		if s.MessageTypes != nil {
			out[i].MessageTypes = append([]uint32(nil), s.MessageTypes...)
		}
	}
	return out
}

func (n *Node) String() string {
	if n == nil {
		return "<nil>"
	}
	name := n.Name
	if name == "" {
		name = n.InstanceID
	}
	return fmt.Sprintf("node(id=%d name=%s type=%s addr=%s:%d state=%s)", n.ID, name, n.Type, n.Address, n.UDPPort, n.State)
}
