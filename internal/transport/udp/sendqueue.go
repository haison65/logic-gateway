package udp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	defaultSendBatchMax  = 64
	defaultSendBatchWait = 100 * time.Microsecond
)

type sendJob struct {
	payload []byte
	release func()
	addr    *net.UDPAddr
	errCh   chan error
}

var sendJobPool = sync.Pool{
	New: func() any {
		return &sendJob{errCh: make(chan error, 1)}
	},
}

func acquireSendJob(payload []byte, release func(), addr *net.UDPAddr) *sendJob {
	j := sendJobPool.Get().(*sendJob)
	// Drain leftover (cancel path may leave a value unread by waiter race).
	select {
	case <-j.errCh:
	default:
	}
	j.payload = payload
	j.release = release
	j.addr = addr
	return j
}

func releaseSendJob(j *sendJob) {
	if j == nil {
		return
	}
	j.payload = nil
	j.release = nil
	j.addr = nil
	select {
	case <-j.errCh:
	default:
	}
	sendJobPool.Put(j)
}

// sendBatcher gom nhiều Send đồng thời → một lần flush (P1.1).
type sendBatcher struct {
	conn *Conn
	ch   chan *sendJob
	max  int
	wait time.Duration

	stopOnce sync.Once
	stopped  chan struct{}
	wg       sync.WaitGroup
}

func startSendBatcher(c *Conn, max int, wait time.Duration) *sendBatcher {
	if max <= 0 {
		max = defaultSendBatchMax
	}
	if wait <= 0 {
		wait = defaultSendBatchWait
	}
	b := &sendBatcher{
		conn:    c,
		ch:      make(chan *sendJob, max*4),
		max:     max,
		wait:    wait,
		stopped: make(chan struct{}),
	}
	b.wg.Add(1)
	go b.loop()
	return b
}

func (b *sendBatcher) loop() {
	defer b.wg.Done()
	buf := make([]*sendJob, 0, b.max)
	timer := time.NewTimer(b.wait)
	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	stopTimer()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		b.conn.writePayloadsBatch(buf)
		buf = buf[:0]
	}

	for {
		if len(buf) == 0 {
			select {
			case <-b.stopped:
				return
			case job := <-b.ch:
				buf = append(buf, job)
			}
			// Dưới tải: hút thêm job sẵn có — đủ max thì flush ngay, khỏi chờ SendBatchWait.
			buf = b.drain(buf)
			if len(buf) >= b.max {
				flush()
				continue
			}
			timer.Reset(b.wait)
			continue
		}
		select {
		case <-b.stopped:
			flush()
			return
		case job := <-b.ch:
			buf = append(buf, job)
			buf = b.drain(buf)
			if len(buf) >= b.max {
				stopTimer()
				flush()
			}
		case <-timer.C:
			flush()
		}
	}
}

// drain non-blocking hút thêm job vào buf đến max.
func (b *sendBatcher) drain(buf []*sendJob) []*sendJob {
	for len(buf) < b.max {
		select {
		case job := <-b.ch:
			buf = append(buf, job)
		default:
			return buf
		}
	}
	return buf
}

func (b *sendBatcher) submit(ctx context.Context, payload []byte, release func(), addr *net.UDPAddr) error {
	job := acquireSendJob(payload, release, addr)
	select {
	case <-b.stopped:
		if release != nil {
			release()
		}
		releaseSendJob(job)
		return ErrTransportClosed
	case <-ctx.Done():
		if release != nil {
			release()
		}
		releaseSendJob(job)
		return ctx.Err()
	case b.ch <- job:
	}
	select {
	case <-ctx.Done():
		err := <-job.errCh
		releaseSendJob(job)
		if err != nil {
			return err
		}
		return fmt.Errorf("send UDP packet: %w", ctx.Err())
	case err := <-job.errCh:
		releaseSendJob(job)
		return err
	}
}

func (b *sendBatcher) close() {
	b.stopOnce.Do(func() {
		close(b.stopped)
		b.wg.Wait()
	})
}
