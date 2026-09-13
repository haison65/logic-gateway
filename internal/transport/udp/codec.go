package udp

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

var encodePool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024)
		return &b
	},
}

const maxPooledEncodeCap = 256 << 10 // 256 KiB

// encode tuần tự hóa Envelope vào buffer pooled. Caller phải releaseEncode sau khi ghi xong.
func encode(msg *pb.Envelope) ([]byte, func(), error) {
	bp := encodePool.Get().(*[]byte)
	out, err := proto.MarshalOptions{}.MarshalAppend((*bp)[:0], msg)
	if err != nil {
		encodePool.Put(bp)
		return nil, nil, fmt.Errorf("encode envelope: %w", err)
	}
	*bp = out
	release := func() {
		if cap(*bp) == 0 || cap(*bp) > maxPooledEncodeCap {
			return
		}
		*bp = (*bp)[:0]
		encodePool.Put(bp)
	}
	return out, release, nil
}

var decodeEnvPool = sync.Pool{
	New: func() any { return &pb.Envelope{} },
}

// decode giải mã payload protobuf thành Envelope pooled.
// Caller phải ReleaseEnvelope khi không còn dùng (kể cả payload).
func decode(data []byte) (*pb.Envelope, error) {
	msg := decodeEnvPool.Get().(*pb.Envelope)
	msg.Reset()
	if err := proto.Unmarshal(data, msg); err != nil {
		ReleaseEnvelope(msg)
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return msg, nil
}

// ReleaseEnvelope trả Envelope về decode pool. An toàn với nil.
func ReleaseEnvelope(msg *pb.Envelope) {
	if msg == nil {
		return
	}
	msg.Reset()
	decodeEnvPool.Put(msg)
}
