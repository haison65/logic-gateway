//go:build linux

package udp

import (
	"context"
	"fmt"
	"net"

	"golang.org/x/net/ipv4"
)

// SendBatch dùng ipv4.PacketConn.WriteBatch → sendmmsg trên Linux (P2).
// IPv6 hoặc lỗi batch → fallback tuần tự.
func (c *Conn) SendBatch(ctx context.Context, items []EnvelopeAndAddr) error {
	if c == nil {
		return ErrTransportClosed
	}
	if err := c.checkOpen(ctx); err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}
	if len(items) == 1 {
		return c.Send(ctx, items[0].Msg, items[0].Addr)
	}

	type prepared struct {
		payload []byte
		release func()
		addr    *net.UDPAddr
	}
	prep := make([]prepared, 0, len(items))
	defer func() {
		for _, p := range prep {
			if p.release != nil {
				p.release()
			}
		}
	}()

	allV4 := true
	for i, it := range items {
		if it.Msg == nil || it.Addr == nil {
			return fmt.Errorf("%w: nil entry at %d", ErrInvalidMessage, i)
		}
		payload, release, err := encode(it.Msg)
		if err != nil {
			return err
		}
		if len(payload) > c.maxPacketSize {
			release()
			return fmt.Errorf("%w: encoded size %d exceeds max %d", ErrPacketTooLarge, len(payload), c.maxPacketSize)
		}
		if it.Addr.IP != nil && it.Addr.IP.To4() == nil {
			allV4 = false
		}
		prep = append(prep, prepared{payload: payload, release: release, addr: it.Addr})
	}

	if !allV4 {
		for _, p := range prep {
			if _, err := c.conn.WriteToUDP(p.payload, p.addr); err != nil {
				return fmt.Errorf("send UDP packet: %w", err)
			}
		}
		return nil
	}

	pc := ipv4.NewPacketConn(c.conn)
	ms := make([]ipv4.Message, len(prep))
	for i, p := range prep {
		ms[i] = ipv4.Message{
			Buffers: [][]byte{p.payload},
			Addr:    p.addr,
		}
	}
	n, err := pc.WriteBatch(ms, 0)
	if err != nil || n < len(ms) {
		// Fallback phần còn lại / toàn bộ.
		start := n
		if err != nil {
			start = 0
		}
		for i := start; i < len(prep); i++ {
			if _, werr := c.conn.WriteToUDP(prep[i].payload, prep[i].addr); werr != nil {
				return fmt.Errorf("send UDP packet: %w", werr)
			}
		}
	}
	return nil
}
