package udp

import (
	"context"
	"net"
	"sync"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

// Packet pool cho datagram đã copy khỏi socket buffer.
// ReceiveRaw cấp; caller (dispatch worker) phải ReleasePacket sau decode.
var packetPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 2048)
		return &b
	},
}

const maxPooledPacketCap = DefaultMaxPacketSize

func acquirePacket(n int) []byte {
	if n <= 0 {
		return nil
	}
	bp := packetPool.Get().(*[]byte)
	if cap(*bp) < n {
		*bp = make([]byte, n)
	} else {
		*bp = (*bp)[:n]
	}
	return *bp
}

// ReleasePacket trả buffer về pool. Không dùng slice sau khi gọi.
func ReleasePacket(b []byte) {
	if b == nil {
		return
	}
	if cap(b) == 0 || cap(b) > maxPooledPacketCap {
		return
	}
	b = b[:0]
	packetPool.Put(&b)
}

// DecodeEnvelope giải mã payload protobuf thành Envelope (export cho worker ngoài receive loop).
func DecodeEnvelope(data []byte) (*pb.Envelope, error) {
	return decode(data)
}

// RawReceiver là optional: receive loop chỉ Read+copy, decode ở worker.
type RawReceiver interface {
	ReceiveRaw(ctx context.Context) ([]byte, *net.UDPAddr, error)
}

// RawBatchReceiver drain nhiều datagram mỗi lần (Linux: recvmmsg qua ReadBatch).
type RawBatchReceiver interface {
	RawReceiver
	ReceiveRawBatch(ctx context.Context, max int) ([][]byte, []*net.UDPAddr, error)
	ReceiveBatchSize() int
}
