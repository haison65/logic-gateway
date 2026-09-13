//go:build !linux

package udp

import "fmt"

func (c *Conn) writePayloadsBatch(jobs []*sendJob) {
	for _, j := range jobs {
		c.writeOneJob(j)
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
