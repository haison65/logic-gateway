package outbound

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func startH2C(t *testing.T, handler http.HandlerFunc) (baseURL string, shutdown func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h2s := &http2.Server{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/data", handler)
	srv := &http.Server{Handler: h2c.NewHandler(mux, h2s)}
	go func() { _ = srv.Serve(ln) }()
	return "http://" + ln.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}

func TestForwardHTTP2Success(t *testing.T) {
	t.Parallel()
	var (
		mu       sync.Mutex
		gotProto string
		gotMsg   string
		gotTx    string
	)
	base, stop := startH2C(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotProto = r.Proto
		gotMsg = r.Header.Get("X-Message-Id")
		gotTx = r.Header.Get("X-Transaction-Id")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("pong"))
	})
	defer stop()

	c := New(base, 3*time.Second, nil)
	got, err := c.Forward(context.Background(), sampleEnv())
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus() != 200 || string(got.GetPayload()) != "pong" || got.GetMessageId() != 1001 {
		t.Fatalf("%+v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotProto != "HTTP/2.0" {
		t.Fatalf("spec §12 yêu cầu HTTP/2 Client; server nhận proto=%q", gotProto)
	}
	if gotMsg != "1001" || gotTx != "42" {
		t.Fatalf("correlation msg=%s tx=%s", gotMsg, gotTx)
	}
}

func TestForwardHTTP2StatusCodes(t *testing.T) {
	t.Parallel()
	for _, code := range []int{400, 503} {
		code := code
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			t.Parallel()
			var proto string
			base, stop := startH2C(t, func(w http.ResponseWriter, r *http.Request) {
				proto = r.Proto
				w.WriteHeader(code)
				_, _ = w.Write([]byte("err"))
			})
			defer stop()
			c := New(base, 3*time.Second, nil)
			got, err := c.Forward(context.Background(), sampleEnv())
			if err != nil {
				t.Fatal(err)
			}
			if int(got.GetStatus()) != code {
				t.Fatalf("status %d", got.GetStatus())
			}
			if proto != "HTTP/2.0" {
				t.Fatalf("proto=%q", proto)
			}
		})
	}
}

func TestForwardHTTP2Timeout(t *testing.T) {
	t.Parallel()
	base, stop := startH2C(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	defer stop()
	c := New(base, 40*time.Millisecond, nil)
	_, err := c.Forward(context.Background(), sampleEnv())
	if err == nil {
		t.Fatal("expected timeout")
	}
}

func TestForwardHTTP2ConnectionFailure(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	c := New("http://"+addr, 500*time.Millisecond, nil)
	_, err = c.Forward(context.Background(), sampleEnv())
	if err == nil {
		t.Fatal("expected connection error")
	}
}

func TestForwardParentContextCancel(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	base, stop := startH2C(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	})
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	c := New(base, 5*time.Second, nil)
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Forward(ctx, sampleEnv())
		errCh <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not reached")
	}
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected cancel error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Forward blocked after parent cancel — shutdown leak")
	}
}

func TestNewHTTP2ClientIsHTTP2Transport(t *testing.T) {
	t.Parallel()
	c := newHTTP2Client(time.Second, false)
	_, ok := c.Transport.(*http2.Transport)
	if !ok {
		t.Fatalf("transport %T, want *http2.Transport", c.Transport)
	}
}
