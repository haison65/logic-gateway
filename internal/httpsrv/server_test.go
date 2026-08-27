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
