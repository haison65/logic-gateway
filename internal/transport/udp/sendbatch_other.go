//go:build !linux

package udp

import "context"

// SendBatch gửi tuần tự trên non-Linux (không có sendmmsg tối ưu).
func (c *Conn) SendBatch(ctx context.Context, items []EnvelopeAndAddr) error {
	if c == nil {
		return ErrTransportClosed
	}
	if len(items) == 0 {
		return nil
	}
	return c.sendBatchSequential(ctx, items)
}
