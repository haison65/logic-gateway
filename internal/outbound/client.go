// Package outbound là HTTP/2 client của HTTP2GW cho chiều Logic → remote (§12).
//
// Design yêu cầu HTTP/2 Client, không quy định TLS hay h2c.
// http:// → h2c prior-knowledge (cùng kiểu HTTP server inbound của gateway).
// https:// → HTTP/2 TLS.
// URL/method/path không có trong spec: POST {base}/v1/data + header X-* (cùng inbound).
package outbound

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
	"golang.org/x/net/http2"
)

const maxBody = 1 << 20
const defaultPath = "/v1/data"

// Doer thực hiện HTTP; test thay bằng fake.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client gửi DATA_REQUEST ra remote HTTP/2.
type Client struct {
	baseURL string
	path    string
	doer    Doer
	timeout time.Duration
}

// New tạo client. baseURL rỗng thì Forward trả lỗi. doer nil thì dùng HTTP/2 theo scheme.
func New(baseURL string, timeout time.Duration, doer Doer) *Client {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if doer == nil {
		doer = newHTTP2Client(timeout, isHTTPS(baseURL))
	}
	return &Client{
		baseURL: baseURL,
		path:    defaultPath,
		doer:    doer,
		timeout: timeout,
	}
}

func isHTTPS(baseURL string) bool {
	u, err := url.Parse(baseURL)
	return err == nil && strings.EqualFold(u.Scheme, "https")
}

// newHTTP2Client luôn dùng x/net/http2.Transport — không fallback HTTP/1.1.
func newHTTP2Client(timeout time.Duration, https bool) *http.Client {
	tr := &http2.Transport{}
	if !https {
		tr.AllowHTTP = true
		tr.DialTLSContext = func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		}
	}
	return &http.Client{Timeout: timeout, Transport: tr}
}

// Configured là true khi có remote URL.
func (c *Client) Configured() bool {
	return c != nil && c.baseURL != ""
}

// Forward POST payload tới remote. HTTP 4xx/5xx vẫn là DATA_RESPONSE (status = mã HTTP).
func (c *Client) Forward(ctx context.Context, env *pb.Envelope) (*pb.DataResponse, error) {
	if c == nil || c.baseURL == "" {
		return nil, fmt.Errorf("outbound remote URL is not configured")
	}
	if env == nil || env.GetDataRequest() == nil {
		return nil, fmt.Errorf("require DATA_REQUEST")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	in := env.GetDataRequest()
	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.baseURL+c.path, bytes.NewReader(in.GetPayload()))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/octet-stream")
	httpReq.Header.Set("X-Message-Id", strconv.FormatUint(uint64(in.GetMessageId()), 10))
	if sid := strings.TrimSpace(in.GetSessionId()); sid != "" {
		httpReq.Header.Set("X-Session-Id", sid)
	}
	if tid := strings.TrimSpace(env.GetTraceId()); tid != "" {
		httpReq.Header.Set("X-Trace-Id", tid)
	}
	if env.GetTransactionId() != 0 {
		httpReq.Header.Set("X-Transaction-Id", strconv.FormatUint(env.GetTransactionId(), 10))
	}

	resp, err := c.doer.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxBody {
		body = body[:maxBody]
	}
	status := uint32(resp.StatusCode)
	if status < 100 || status > 599 {
		status = 502
	}
	return &pb.DataResponse{
		MessageId: in.GetMessageId(),
		Status:    status,
		Payload:   body,
	}, nil
}
