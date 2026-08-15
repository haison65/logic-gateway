package logicnode

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/config"
	"github.com/haison65/logic-gateway/internal/dispatch"
	"github.com/haison65/logic-gateway/internal/heartbeat"
	"github.com/haison65/logic-gateway/internal/httpsrv"
	"github.com/haison65/logic-gateway/internal/registration"
	"github.com/haison65/logic-gateway/internal/registry"
	"github.com/haison65/logic-gateway/internal/router"
	"github.com/haison65/logic-gateway/internal/transaction"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

// TestHTTPClientEchoViaLogic khép vòng:
//
//	Client  --HTTP/2-->  http2gw  --UDP DATA_REQUEST-->  Logic
//	Client  <--HTTP/2--  http2gw  <--UDP DATA_RESPONSE-- Logic
func TestHTTPClientEchoViaLogic(t *testing.T) {
	t.Parallel()
	log := zap.NewNop()
	gw := newUDP(t)
	gwAddr := gw.LocalAddr().(*net.UDPAddr)

	reg := registry.NewMemory()
	txMgr := transaction.NewManager()
	rt := router.New(reg, router.NewRoundRobin())
	txSvc, err := transaction.NewService(txMgr, rt, gw, transaction.Config{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = txSvc.Close() })

	disp, err := dispatch.New(1, gw, registration.NewService(reg), heartbeat.NewService(reg, nil), txSvc, log, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	httpSrv, err := httpsrv.New(httpsrv.Config{NodeID: 1, Listen: "127.0.0.1", Port: 0}, txSvc, log, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	go func() { _ = disp.Run(ctx) }()
	go func() { _ = httpSrv.Serve() }()
	t.Cleanup(func() {
		shut, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = httpSrv.Shutdown(shut)
	})

	cfg := config.Logic{
		Node:      config.Node{NodeID: 2, InstanceID: "logic-e2e", IP: "127.0.0.1"},
		UDP:       config.UDP{Listen: "127.0.0.1", Port: 0},
		Gateway:   config.Gateway{Host: gwAddr.IP.String(), Port: gwAddr.Port},
		Heartbeat: config.Heartbeat{Interval: config.Duration(20 * time.Millisecond)},
		Services:  []config.Service{{ServiceID: 100, ServiceName: "call", MessageTypes: []uint32{1001}}},
	}
	go func() { _ = Run(ctx, cfg, log) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		n, ok := reg.Get(2)
		if ok && n.State == registry.NodeStateActive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("logic not ACTIVE: ok=%v node=%+v", ok, n)
		}
		time.Sleep(10 * time.Millisecond)
	}

	var addr string
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		addr = httpSrv.Addr()
		if addr != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if addr == "" {
		t.Fatal("http not listening")
	}

	payload := []byte("hello-http2-udp")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/v1/data", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Message-Id", "1001")
	req.Header.Set("X-Session-Id", "s1")
	req.Header.Set("X-Trace-Id", "e2e-h2")
	req.Proto = "HTTP/2.0"
	req.ProtoMajor = 2
	req.ProtoMinor = 0

	resp, err := http2H2CClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProtoMajor != 2 {
		t.Fatalf("client↔http2gw phải là HTTP/2, proto=%s major=%d", resp.Proto, resp.ProtoMajor)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("echo mismatch: got %q want %q", body, payload)
	}
	if resp.Header.Get("X-Transaction-Id") == "" {
		t.Fatal("missing X-Transaction-Id — UDP DATA_RESPONSE chưa được ghép")
	}
	if resp.Header.Get("X-Message-Id") != "1001" {
		t.Fatalf("X-Message-Id = %s", resp.Header.Get("X-Message-Id"))
	}
}

// http2H2CClient nói HTTP/2 prior-knowledge (h2c) với http2gw, không TLS.
func http2H2CClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		},
	}
}
