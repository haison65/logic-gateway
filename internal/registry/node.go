// Package registry lưu trạng thái các node Logic đã đăng ký.
//
// Gói này độc lập với UDP, protobuf và HTTP. Định danh logic là Node.ID (node_id).
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
	default:
		return "unspecified"
	}
}

// Type là vai trò node ở tầng domain (không dùng enum protobuf).
type Type uint32

const (
	TypeUnspecified Type = 0
	TypeHTTP2GW     Type = 1
	TypeLogic       Type = 2
)

// Service mô tả một năng lực dịch vụ mà node quảng bá.
type Service struct {
	ID           uint32
	Name         string
	MessageTypes []uint32
}

// Node là trạng thái đăng ký trong Registry, không phải message wire.
type Node struct {
	ID                uint32
	InstanceID        string
	Type              Type
	Address           string
	UDPPort           uint32
	Services          []Service
	State             NodeState
	LastHeartbeatAt   time.Time
	Load              uint32
	ActiveTransaction uint32
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
	return fmt.Sprintf("node(id=%d instance=%s addr=%s:%d state=%s)", n.ID, n.InstanceID, n.Address, n.UDPPort, n.State)
}
