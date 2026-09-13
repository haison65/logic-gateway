//go:build !linux

package udp

import (
	"context"
	"net"
	"time"
)

// ReceiveRawBatch: non-Linux — 1 packet blocking rồi drain non-blocking tới max.
func (c *Conn) ReceiveRawBatch(ctx context.Context, max int) ([][]byte, []*net.UDPAddr, error) {
	if c == nil {
		return nil, nil, ErrTransportClosed
	}
	if max <= 0 {
		max = c.ReceiveBatchSize()
	}

	pkt, addr, err := c.ReceiveRaw(ctx)
	if err != nil {
		return nil, nil, err
	}
	pkts := [][]byte{pkt}
	addrs := []*net.UDPAddr{addr}
	if max <= 1 {
		return pkts, addrs, nil
	}

	for len(pkts) < max {
		if err := c.checkOpen(ctx); err != nil {
			break
		}
		_ = c.conn.SetReadDeadline(time.Now())
		n, a, rerr := c.conn.ReadFromUDP(c.readBuf)
		if rerr != nil {
			break
		}
		p := acquirePacket(n)
		copy(p, c.readBuf[:n])
		pkts = append(pkts, p)
		addrs = append(addrs, a)
	}
	return pkts, addrs, nil
}
