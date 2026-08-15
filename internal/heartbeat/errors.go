package heartbeat

import "errors"

var (
	// ErrInvalidConfig được trả về khi cấu hình heartbeat không hợp lệ.
	ErrInvalidConfig = errors.New("invalid heartbeat config")

	// ErrInvalidHeartbeat được trả về khi HeartbeatRequest không hợp lệ.
	ErrInvalidHeartbeat = errors.New("invalid heartbeat request")

	// ErrUnknownNode được trả về khi heartbeat tới từ node chưa đăng ký.
	ErrUnknownNode = errors.New("unknown heartbeat node")

	// ErrDeadNode được trả về khi node DEAD gửi heartbeat; cần đăng ký lại.
	ErrDeadNode = errors.New("dead node cannot heartbeat")

	// ErrInvalidTransition được trả về khi chuyển trạng thái không hợp lệ.
	ErrInvalidTransition = errors.New("invalid node state transition")
)
