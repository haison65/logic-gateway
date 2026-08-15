package registration

import (
	"fmt"
	"net"
	"strings"
	"unicode/utf8"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

// Validate kiểm tra các trường bắt buộc của RegisterRequest trước khi ghi Registry.
func Validate(req *pb.RegisterRequest) error {
	if req == nil {
		return fmt.Errorf("%w: request is nil", ErrInvalidRequest)
	}
	if req.GetNodeId() == 0 {
		return fmt.Errorf("%w: node_id is required", ErrInvalidRequest)
	}
	switch req.GetNodeType() {
	case pb.NodeType_NODE_TYPE_LOGIC, pb.NodeType_NODE_TYPE_HTTP2GW:
	default:
		return fmt.Errorf("%w: node_type is required", ErrInvalidRequest)
	}
	if strings.TrimSpace(req.GetInstanceId()) == "" {
		return fmt.Errorf("%w: instance_id is required", ErrInvalidRequest)
	}
	if !utf8.ValidString(req.GetInstanceId()) {
		return fmt.Errorf("%w: instance_id is not valid UTF-8", ErrInvalidRequest)
	}
	ip := strings.TrimSpace(req.GetIp())
	if ip == "" {
		return fmt.Errorf("%w: ip is required", ErrInvalidRequest)
	}
	if net.ParseIP(ip) == nil {
		return fmt.Errorf("%w: ip is not a valid IP address", ErrInvalidRequest)
	}
	if req.GetPort() == 0 || req.GetPort() > 65535 {
		return fmt.Errorf("%w: port must be in 1..65535", ErrInvalidRequest)
	}
	for i, svc := range req.GetServices() {
		if svc == nil || svc.GetServiceId() == 0 {
			return fmt.Errorf("%w: services[%d].service_id is required", ErrInvalidRequest, i)
		}
	}
	return nil
}
