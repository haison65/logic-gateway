package outbound

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

type fakeDoer struct {
	status int
	body   []byte
	err    error
	delay  time.Duration
	last   *http.Request
}

func (f *fakeDoer) Do(req *http.Request) (*http.Response, error) {
	f.last = req
	if f.delay > 0 {
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(f.delay):
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return &http.Response{
		StatusCode: f.status,
		Body:       io.NopCloser(bytes.NewReader(f.body)),
		Header:     make(http.Header),
	}, nil
}

func sampleEnv() *pb.Envelope {
	return &pb.Envelope{
		TransactionId: 42,
		TraceId:       "tr-1",
		Type:          pb.MessageType_MESSAGE_TYPE_DATA_REQUEST,
		Body: &pb.Envelope_DataRequest{DataRequest: &pb.DataRequest{
			MessageId: 1001,
			SessionId: "sess",
			Payload:   []byte("hello"),
		}},
	}
}

func TestForwardSuccess(t *testing.T) {
	t.Parallel()
	d := &fakeDoer{status: 200, body: []byte("ok")}
	c := New("http://remote.example", time.Second, d)
	got, err := c.Forward(context.Background(), sampleEnv())
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus() != 200 || string(got.GetPayload()) != "ok" || got.GetMessageId() != 1001 {
		t.Fatalf("%+v", got)
	}
	if d.last.Method != http.MethodPost || d.last.URL.Path != "/v1/data" {
		t.Fatalf("req %s %s", d.last.Method, d.last.URL)
	}
	if d.last.Header.Get("X-Message-Id") != "1001" || d.last.Header.Get("X-Transaction-Id") != "42" {
		t.Fatalf("headers %+v", d.last.Header)
	}
}

func TestForwardHTTPStatus(t *testing.T) {
	t.Parallel()
	for _, code := range []int{400, 503} {
		d := &fakeDoer{status: code, body: []byte("err")}
		c := New("http://remote.example", time.Second, d)
		got, err := c.Forward(context.Background(), sampleEnv())
		if err != nil {
			t.Fatal(err)
		}
		if int(got.GetStatus()) != code {
			t.Fatalf("status %d", got.GetStatus())
		}
	}
}

func TestForwardTimeout(t *testing.T) {
	t.Parallel()
	d := &fakeDoer{status: 200, delay: time.Second}
	c := New("http://remote.example", 20*time.Millisecond, d)
	_, err := c.Forward(context.Background(), sampleEnv())
	if err == nil {
		t.Fatal("expected timeout")
	}
}

func TestForwardConnectionError(t *testing.T) {
	t.Parallel()
	d := &fakeDoer{err: errors.New("connection refused")}
	c := New("http://remote.example", time.Second, d)
	_, err := c.Forward(context.Background(), sampleEnv())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNotConfigured(t *testing.T) {
	t.Parallel()
	c := New("", time.Second, &fakeDoer{})
	if c.Configured() {
		t.Fatal("expected not configured")
	}
	_, err := c.Forward(context.Background(), sampleEnv())
	if err == nil {
		t.Fatal("expected error")
	}
}
