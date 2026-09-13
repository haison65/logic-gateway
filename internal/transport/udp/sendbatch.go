package udp

import (
	"context"
	"fmt"
	"net"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

// EnvelopeAndAddr cặp Envelope + đích cho SendBatch (P2).
type EnvelopeAndAddr struct {
	Msg  *pb.Envelope
	Addr *net.UDPAddr
}

// sendBatchSequential gửi từng datagram (fallback mọi OS / IPv6).
func (c *Conn) sendBatchSequential(ctx context.Context, items []EnvelopeAndAddr) error {
	for i := range items {
		if err := c.Send(ctx, items[i].Msg, items[i].Addr); err != nil {
			return fmt.Errorf("send batch[%d]: %w", i, err)
		}
	}
	return nil
}
