//go:build linux

package udp

import (
	"context"
	"fmt"
	"net"
	"time"

	"golang.org/x/net/ipv4"
)

func (c *Conn) ensureReadBatchBufs(max int) {
	if len(c.readBatchBufs) >= max {
		return
	}
	bufs := make([][]byte, max)
	for i := 0; i < max; i++ {
		if i < len(c.readBatchBufs) {
			bufs[i] = c.readBatchBufs[i]
			continue
		}
		bufs[i] = make([]byte, c.maxPacketSize)
	}
	c.readBatchBufs = bufs
}

// ReceiveRawBatch đọc tới max datagram bằng ReadBatch (recvmmsg). Case B / 1 CPU.
func (c *Conn) ReceiveRawBatch(ctx context.Context, max int) ([][]byte, []*net.UDPAddr, error) {
	if c == nil {
		return nil, nil, ErrTransportClosed
	}
	if max <= 0 {
		max = c.ReceiveBatchSize()
	}
	if max == 1 {
		pkt, addr, err := c.ReceiveRaw(ctx)
		if err != nil {
			return nil, nil, err
		}
		return [][]byte{pkt}, []*net.UDPAddr{addr}, nil
	}

	c.ensureReadBatchBufs(max)
	ms := make([]ipv4.Message, max)
	for i := 0; i < max; i++ {
		ms[i].Buffers = [][]byte{c.readBatchBufs[i]}
	}
	pc := ipv4.NewPacketConn(c.conn)

	for {
		if err := c.checkOpen(ctx); err != nil {
			return nil, nil, err
		}

		deadline := time.Now().Add(c.readPollInterval)
		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}
		if err := c.conn.SetReadDeadline(deadline); err != nil {
			if c.isClosed() {
				return nil, nil, ErrTransportClosed
			}
			return nil, nil, fmt.Errorf("set UDP read deadline: %w", err)
		}

		n, err := pc.ReadBatch(ms, 0)
		if err != nil {
			if c.isClosed() {
				return nil, nil, ErrTransportClosed
			}
			if isTimeout(err) {
				if ctx.Err() != nil {
					return nil, nil, fmt.Errorf("receive UDP packet: %w", ctx.Err())
				}
				continue
			}
			// ReadBatch không hỗ trợ → fallback từng packet.
			pkt, addr, rerr := c.ReceiveRaw(ctx)
			if rerr != nil {
				return nil, nil, rerr
			}
			return [][]byte{pkt}, []*net.UDPAddr{addr}, nil
		}
		if n <= 0 {
			continue
		}

		pkts := make([][]byte, 0, n)
		addrs := make([]*net.UDPAddr, 0, n)
		for i := 0; i < n; i++ {
			nn := ms[i].N
			if nn <= 0 {
				continue
			}
			pkt := acquirePacket(nn)
			copy(pkt, c.readBatchBufs[i][:nn])
			pkts = append(pkts, pkt)
			ua, _ := ms[i].Addr.(*net.UDPAddr)
			addrs = append(addrs, ua)
		}
		if len(pkts) == 0 {
			continue
		}
		return pkts, addrs, nil
	}
}
