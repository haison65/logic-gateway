//go:build linux

package udp

import (
	"fmt"

	"golang.org/x/net/ipv4"
)

// writePayloadsBatch ghi nhiều datagram đã encode (WriteBatch → sendmmsg khi IPv4).
func (c *Conn) writePayloadsBatch(jobs []*sendJob) {
	if len(jobs) == 0 {
		return
	}
	if len(jobs) == 1 {
		c.writeOneJob(jobs[0])
		return
	}

	allV4 := true
	for _, j := range jobs {
		if j.addr == nil || j.addr.IP == nil || j.addr.IP.To4() == nil {
			allV4 = false
			break
		}
	}
	if !allV4 {
		for _, j := range jobs {
			c.writeOneJob(j)
		}
		return
	}

	pc := ipv4.NewPacketConn(c.conn)
	ms := make([]ipv4.Message, len(jobs))
	for i, j := range jobs {
		ms[i] = ipv4.Message{
			Buffers: [][]byte{j.payload},
			Addr:    j.addr,
		}
	}
	n, err := pc.WriteBatch(ms, 0)
	if err != nil || n < len(ms) {
		start := n
		if err != nil {
			start = 0
		}
		for i := 0; i < start; i++ {
			if jobs[i].release != nil {
				jobs[i].release()
			}
			jobs[i].errCh <- nil
		}
		for i := start; i < len(jobs); i++ {
			c.writeOneJob(jobs[i])
		}
		return
	}
	for _, j := range jobs {
		if j.release != nil {
			j.release()
		}
		j.errCh <- nil
	}
}

func (c *Conn) writeOneJob(j *sendJob) {
	n, err := c.conn.WriteToUDP(j.payload, j.addr)
	if err == nil && n != len(j.payload) {
		err = fmt.Errorf("send UDP packet: short write %d/%d", n, len(j.payload))
	}
	if j.release != nil {
		j.release()
	}
	j.errCh <- err
}
