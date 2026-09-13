package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/time/rate"
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
	failLogPath   string
	failLogMax    int
	summaryLogPath string
	clientName    string
	clientCPUs    float64
}

type callResult struct {
	status     int
	protoMajor int
	latency    time.Duration
	txID       string
	traceID    string
	sessionID  string
	body       []byte
	err        error
}

func newH2CClient(timeout time.Duration) *http.Client {
	// F2: nhiều HTTP/2 connection / host để vượt giới hạn stream/conn.
	const conns = 4
	transports := make([]*http2.Transport, conns)
	for i := 0; i < conns; i++ {
		transports[i] = &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		}
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: &roundRobinH2Transport{ts: transports},
	}
}

// roundRobinH2Transport xoay vòng nhiều http2.Transport (mỗi cái ~1 conn/host).
type roundRobinH2Transport struct {
	ts []*http2.Transport
	n  atomic.Uint64
}

func (r *roundRobinH2Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if r == nil || len(r.ts) == 0 {
		return nil, fmt.Errorf("http2 transport not configured")
	}
	i := r.n.Add(1)
	return r.ts[int(i%uint64(len(r.ts)))].RoundTrip(req)
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
		return callResult{latency: time.Since(start), traceID: trace, sessionID: session, err: err}
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return callResult{
			status:     resp.StatusCode,
			protoMajor: resp.ProtoMajor,
			latency:    time.Since(start),
			txID:       resp.Header.Get("X-Transaction-Id"),
			traceID:    trace,
			sessionID:  session,
			err:        err,
		}
	}
	return callResult{
		status:     resp.StatusCode,
		protoMajor: resp.ProtoMajor,
		latency:    time.Since(start),
		txID:       resp.Header.Get("X-Transaction-Id"),
		traceID:    trace,
		sessionID:  session,
		body:       out,
	}
}

func newQPSLimiter(qps float64) *rate.Limiter {
	if qps <= 0 {
		return nil
	}
	// burst=1 → khoảng cách đều ~1/qps giữa các lần được phép start (không dồn burst).
	return rate.NewLimiter(rate.Limit(qps), 1)
}

func runLoad(ctx context.Context, client *http.Client, opt options) stats {
	var (
		seq     atomic.Int64
		stopped atomic.Bool
		st      stats
		mu      sync.Mutex
		wg      sync.WaitGroup
	)

	fl, err := newFailLogger(opt.failLogPath, opt.failLogMax)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail log: %v\n", err)
	}
	defer func() {
		fl.close()
		if s := fl.summary(); s != "" {
			fmt.Fprintln(os.Stderr, s)
		}
	}()

	limiter := newQPSLimiter(opt.qps)

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
				// Chờ token đều theo qps; không drop tick như time.Ticker.
				if err := limiter.Wait(ctx); err != nil {
					return
				}
				if ctx.Err() != nil || stopped.Load() {
					return
				}
				if !deadline.IsZero() && time.Now().After(deadline) {
					stopped.Store(true)
					return
				}
			}
			r := doRequest(ctx, client, opt, n)
			mu.Lock()
			st.add(r)
			mu.Unlock()
			failed := r.err != nil || r.status >= 400 || r.status == 0
			if failed {
				fl.log(failEntryFromResult(n, r))
			}
			if opt.verbose && failed {
				fmt.Printf("fail seq=%d status=%d tx=%s trace=%s err=%v\n", n, r.status, r.txID, r.traceID, r.err)
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
