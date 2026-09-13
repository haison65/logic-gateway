package registration

import (
	"fmt"
	"strings"

	"github.com/haison65/logic-gateway/internal/registry"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

// Service đăng ký node vào Registry.
// Không giữ mutable state riêng; an toàn khi gọi đồng thời nếu Registry an toàn.
type Service struct {
	registry registry.Registry
}

// NewService tạo dịch vụ đăng ký. registry không được nil.
func NewService(reg registry.Registry) *Service {
	return &Service{registry: reg}
}

// Register kiểm tra request, lưu Node, và tạo RegisterResponse.
// Khi thất bại, response.Accepted = false và error khác nil (nếu response != nil thì vẫn gửi được cho peer).
func (s *Service) Register(req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if s == nil || s.registry == nil {
		return failResponse(0, "registration service is not configured"), fmt.Errorf("%w: service is not configured", ErrInvalidRequest)
	}

	if err := Validate(req); err != nil {
		return failResponse(reqNodeID(req), err.Error()), err
	}

	node := toNode(req)
	if err := s.registry.Register(node); err != nil {
		return failResponse(req.GetNodeId(), err.Error()), fmt.Errorf("register node: %w", err)
	}

	return &pb.RegisterResponse{
		Accepted:       true,
		AssignedNodeId: req.GetNodeId(),
		Message:        "registered",
	}, nil
}

func toNode(req *pb.RegisterRequest) *registry.Node {
	services := make([]registry.Service, 0, len(req.GetServices()))
	for _, svc := range req.GetServices() {
		if svc == nil {
			continue
		}
		services = append(services, registry.Service{
			ID:           svc.GetServiceId(),
			Name:         svc.GetServiceName(),
			MessageTypes: append([]uint32(nil), svc.GetMessageTypes()...),
		})
	}

	return &registry.Node{
		ID:         req.GetNodeId(),
		InstanceID: strings.TrimSpace(req.GetInstanceId()),
		Name:       strings.TrimSpace(req.GetInstanceId()),
		Type:       registry.Type(req.GetNodeType()),
		Address:    strings.TrimSpace(req.GetIp()),
		UDPPort:    req.GetPort(),
		Services:   services,
	}
}

func failResponse(nodeID uint32, message string) *pb.RegisterResponse {
	return &pb.RegisterResponse{
		Accepted:       false,
		AssignedNodeId: nodeID,
		Message:        message,
	}
}

func reqNodeID(req *pb.RegisterRequest) uint32 {
	if req == nil {
		return 0
	}
	return req.GetNodeId()
}
