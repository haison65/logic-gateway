package heartbeat

import (
	"errors"
	"fmt"

	"github.com/haison65/logic-gateway/internal/registry"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

// Service xử lý HeartbeatRequest: validate, cập nhật Registry, tạo HeartbeatResponse.
// Không đụng socket UDP.
type Service struct {
	registry registry.Registry
	clock    Clock
}

// NewService tạo dịch vụ heartbeat. clock nil thì dùng time.Now.
func NewService(reg registry.Registry, clock Clock) *Service {
	return &Service{registry: reg, clock: resolveClock(clock)}
}

// Handle xử lý một HeartbeatRequest.
// Thành công: HeartbeatResponse (echo sequence, timestamp máy nhận).
// HeartbeatResponse không có cờ lỗi; thất bại trả về error (unknown/DEAD/invalid).
func (s *Service) Handle(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if s == nil || s.registry == nil {
		return nil, fmt.Errorf("%w: service is not configured", ErrInvalidHeartbeat)
	}
	if req == nil || req.GetNodeId() == 0 {
		return nil, fmt.Errorf("%w: node_id is required", ErrInvalidHeartbeat)
	}

	at := s.clock.Now()
	_, err := s.registry.ApplyHeartbeat(req.GetNodeId(), at, NextOnHeartbeat)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, fmt.Errorf("%w: node_id=%d", ErrUnknownNode, req.GetNodeId())
		}
		return nil, err
	}
	_ = s.registry.SetRuntime(req.GetNodeId(), req.GetLoad(), req.GetActiveTransaction())

	ts := at.UnixMilli()
	if ts < 0 {
		ts = 0
	}
	return &pb.HeartbeatResponse{
		Sequence:    req.GetSequence(),
		TimestampMs: uint64(ts),
	}, nil
}
