package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

type options struct {
	addr          string
	messageID     uint
	sessionID     string
	uniqueSession bool
	traceID       string
	body          string
	timeout       time.Duration
	n             int
	c             int
	duration      time.Duration
	qps           float64
	verbose       bool
}

type callResult struct {
	status     int
	protoMajor int
	latency    time.Duration
	txID       string
	body       []byte
	err        error
}

func newH2CClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		},
	}
}

func doRequest(ctx context.Context, client *http.Client, opt options, seq int) callResult {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, opt.addr, bytes.NewReader([]byte(opt.body)))
	if err != nil {
		return callResult{latency: time.Since(start), err: err}
	}
	req.Header.Set("X-Message-Id", strconv.FormatUint(uint64(opt.messageID), 10))
	session := strings.TrimSpace(opt.sessionID)
	if opt.uniqueSession {
		session = fmt.Sprintf("%s-%d", session, seq)
	}
	if session != "" {
		req.Header.Set("X-Session-Id", session)
	}
	trace := strings.TrimSpace(opt.traceID)
	if trace == "" {
		trace = fmt.Sprintf("cli-%d-%d", time.Now().UnixMilli(), seq)
	} else if opt.n > 1 || opt.c > 1 {
		trace = fmt.Sprintf("%s-%d", trace, seq)
	}
	req.Header.Set("X-Trace-Id", trace)
	req.Proto = "HTTP/2.0"
	req.ProtoMajor = 2
	req.ProtoMinor = 0

	resp, err := client.Do(req)
	if err != nil {
		return callResult{latency: time.Since(start), err: err}
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return callResult{status: resp.StatusCode, protoMajor: resp.ProtoMajor, latency: time.Since(start), err: err}
	}
	return callResult{
		status:     resp.StatusCode,
		protoMajor: resp.ProtoMajor,
		latency:    time.Since(start),
		txID:       resp.Header.Get("X-Transaction-Id"),
		body:       out,
	}
}

func runLoad(ctx context.Context, client *http.Client, opt options) stats {
	var (
		seq     atomic.Int64
		stopped atomic.Bool
		st      stats
		mu      sync.Mutex
		wg      sync.WaitGroup
	)

	var limiter <-chan time.Time
	if opt.qps > 0 {
		interval := time.Duration(float64(time.Second) / opt.qps)
		if interval < time.Microsecond {
			interval = time.Microsecond
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		limiter = t.C
	}

	deadline := time.Time{}
	if opt.duration > 0 {
		deadline = time.Now().Add(opt.duration)
	}

	worker := func() {
		defer wg.Done()
		for {
			if ctx.Err() != nil || stopped.Load() {
				return
			}
			if !deadline.IsZero() && time.Now().After(deadline) {
				stopped.Store(true)
				return
			}
			n := int(seq.Add(1))
			if opt.n > 0 && n > opt.n {
				stopped.Store(true)
				return
			}
			if limiter != nil {
				select {
				case <-ctx.Done():
					return
				case <-limiter:
				}
			}
			r := doRequest(ctx, client, opt, n)
			mu.Lock()
			st.add(r)
			mu.Unlock()
			if opt.verbose && (r.err != nil || r.status >= 400) {
				fmt.Printf("fail seq=%d status=%d err=%v\n", n, r.status, r.err)
			}
		}
	}

	wg.Add(opt.c)
	for i := 0; i < opt.c; i++ {
		go worker()
	}
	wg.Wait()
	return st
}
