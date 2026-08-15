package router

import "errors"

var (
	// ErrNoEligibleNode không có node ACTIVE hỗ trợ message_id.
	ErrNoEligibleNode = errors.New("no eligible node")

	// ErrUnsupportedMessageType Envelope không phải DATA cần định tuyến.
	ErrUnsupportedMessageType = errors.New("unsupported message type for routing")

	// ErrEmptyCandidates strategy nhận danh sách rỗng.
	ErrEmptyCandidates = errors.New("empty routing candidates")

	// ErrInvalidRoutingKey key consistent-hash rỗng.
	ErrInvalidRoutingKey = errors.New("invalid routing key")
)
