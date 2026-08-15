package udp

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

// encode tuần tự hóa Envelope thành payload protobuf.
func encode(msg *pb.Envelope) ([]byte, error) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encode envelope: %w", err)
	}
	return payload, nil
}

// decode giải mã payload protobuf thành Envelope.
func decode(data []byte) (*pb.Envelope, error) {
	msg := &pb.Envelope{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return msg, nil
}
