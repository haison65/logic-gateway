package httpsrv

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/metrics"
	"github.com/haison65/logic-gateway/internal/transaction"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
	"go.uber.org/zap"
)

type stubTX struct {
	env *pb.Envelope
	err error
}

func (s stubTX) Request(context.Context, *pb.Envelope) (*pb.Envelope, error) {
	return s.env, s.err
}

// stubTXAssignID gán transaction_id vào request (như Service.Request thật) rồi trả lỗi.
type stubTXAssignID struct {
	id  uint64
	err error
}

func (s stubTXAssignID) Request(_ context.Context, req *pb.Envelope) (*pb.Envelope, error) {
	if req != nil {
		req.TransactionId = s.id
	}
	return nil, s.err
}

func TestDataErrorReturnsTransactionID(t *testing.T) {
	t.Parallel()
	srv, err := New(Config{NodeID: 1, Listen: "127.0.0.1", Port: 0}, stubTXAssignID{
		id:  99,
		err: transaction.ErrTransactionTimeout,
	}, zap.NewNop(), nil)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	addr := waitAddr(t, srv)
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/data", bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Message-Id", "1001")
	got, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = got.Body.Close()
	if got.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d", got.StatusCode)
	}
	if got.Header.Get("X-Transaction-Id") != "99" {
		t.Fatalf("tx header = %q want 99", got.Header.Get("X-Transaction-Id"))
	}
}

func TestDataLogicErrorMapsHTTPStatus(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		code   pb.ErrorCode
		want   int
	}{
		{name: "invalid", code: pb.ErrorCode_ERROR_CODE_INVALID_MESSAGE, want: http.StatusBadRequest},
		{name: "overload", code: pb.ErrorCode_ERROR_CODE_OVERLOAD, want: http.StatusServiceUnavailable},
		{name: "timeout", code: pb.ErrorCode_ERROR_CODE_TIMEOUT, want: http.StatusGatewayTimeout},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, err := New(Config{NodeID: 1, Listen: "127.0.0.1", Port: 0}, stubTX{
				err: &transaction.LogicError{Code: tc.code, Message: tc.name},
			}, zap.NewNop(), nil)
			if err != nil {
				t.Fatal(err)
			}
			errCh := make(chan error, 1)
			go func() { errCh <- srv.Serve() }()
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = srv.Shutdown(ctx)
			})
			addr := waitAddr(t, srv)
			req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/data", bytes.NewReader([]byte("x")))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-Message-Id", "1001")
			got, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = got.Body.Close()
			if got.StatusCode != tc.want {
				t.Fatalf("status = %d want %d", got.StatusCode, tc.want)
			}
		})
	}
}

func waitAddr(t *testing.T, srv *Server) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := srv.Addr(); addr != "" {
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("server did not listen")
	return ""
}

func TestHealthAndData(t *testing.T) {
	t.Parallel()
	resp := &pb.Envelope{
		TransactionId: 11,
		Type:          pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE,
		Body:          &pb.Envelope_DataResponse{DataResponse: &pb.DataResponse{MessageId: 1001, Status: 200, Payload: []byte("ok")}},
	}
	srv, err := New(Config{NodeID: 1, Listen: "127.0.0.1", Port: 0}, stubTX{env: resp}, zap.NewNop(), nil)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	deadline := time.Now().Add(2 * time.Second)
	var addr string
	for time.Now().Before(deadline) {
		addr = srv.Addr()
		if addr != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if addr == "" {
		t.Fatal("server did not listen")
	}

	h, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = h.Body.Close()
	if h.StatusCode != 200 {
		t.Fatalf("health = %d", h.StatusCode)
	}

	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/data", bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Message-Id", "1001")
	req.Header.Set("X-Session-Id", "s1")
	got, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(got.Body)
	_ = got.Body.Close()
	if got.StatusCode != 200 || string(body) != "ok" {
		t.Fatalf("status=%d body=%s", got.StatusCode, body)
	}
	if got.Header.Get("X-Transaction-Id") != "11" {
		t.Fatalf("tx header = %s", got.Header.Get("X-Transaction-Id"))
	}

	mresp, err := http.Get("http://" + addr + "/metrics.json")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(mresp.Body)
	_ = mresp.Body.Close()
	if mresp.StatusCode != 200 {
		t.Fatalf("metrics.json status = %d", mresp.StatusCode)
	}
	var st metrics.RequestStats
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	if st.RequestsTotal != 1 || st.RequestsOK != 1 || st.Latency.Count != 1 {
		t.Fatalf("metrics = %+v body=%s", st, raw)
	}
	if st.Latency.MinMS < 0 || st.Latency.MaxMS < st.Latency.MinMS || st.Latency.AvgMS < 0 {
		t.Fatalf("latency = %+v", st.Latency)
	}

	promResp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	promBody, _ := io.ReadAll(promResp.Body)
	_ = promResp.Body.Close()
	if promResp.StatusCode != 200 {
		t.Fatalf("prometheus /metrics status = %d", promResp.StatusCode)
	}
	if !bytes.Contains(promBody, []byte("http2gw_http_requests_total")) {
		t.Fatalf("prometheus body missing http2gw_http_requests_total: %s", promBody)
	}

	bad, err := http.Post("http://"+addr+"/v1/data", "application/octet-stream", bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	_ = bad.Body.Close()
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad status = %d", bad.StatusCode)
	}
	mresp2, err := http.Get("http://" + addr + "/metrics.json")
	if err != nil {
		t.Fatal(err)
	}
	raw2, _ := io.ReadAll(mresp2.Body)
	_ = mresp2.Body.Close()
	if err := json.Unmarshal(raw2, &st); err != nil {
		t.Fatal(err)
	}
	if st.RequestsFail != 1 || st.FailByCode["400"] != 1 {
		t.Fatalf("fail_by_code = %+v", st.FailByCode)
	}
	if st.FailByReason[metrics.ReasonMissingMessageID] != 1 {
		t.Fatalf("fail_by_reason = %+v", st.FailByReason)
	}
}
