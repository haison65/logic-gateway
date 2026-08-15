package udp

import "errors"

var (
	// ErrTransportClosed được trả về khi thao tác trên transport đã đóng.
	ErrTransportClosed = errors.New("udp transport closed")

	// ErrInvalidMessage được trả về khi datagram không giải mã được thành Envelope.
	ErrInvalidMessage = errors.New("invalid protobuf envelope")

	// ErrPacketTooLarge được trả về khi Envelope mã hóa vượt MaxPacketSize.
	ErrPacketTooLarge = errors.New("udp packet too large")
)
