package httpsrv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/haison65/logic-gateway/internal/logger"
	"github.com/haison65/logic-gateway/internal/metrics"
	"github.com/haison65/logic-gateway/internal/router"
	"github.com/haison65/logic-gateway/internal/transaction"
	"github.com/haison65/logic-gateway/internal/transport/udp"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const maxBody = 1 << 20 // 1 MiB

// Requester gửi DATA_REQUEST và chờ DATA_RESPONSE.
type Requester interface {
	Request(ctx context.Context, req *pb.Envelope) (*pb.Envelope, error)
}

// Config HTTP của gateway.
type Config struct {
	NodeID       uint32
	Listen       string
	Port         int
	TLSCert      string
	TLSKey       string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// MaxRPS: giới hạn admit POST /v1/data (0 = không giới hạn). Dùng Allow() — reject 429, không Wait.
	MaxRPS float64
	// MaxRPSBurst: burst token (0 = mặc định max(1, MaxRPS/10), clamp 2000).
	MaxRPSBurst int
	// MaxPending: reject 429 khi manager_pending vượt ngưỡng (0 = tắt).
	MaxPending int
}

// Server phục vụ HTTP/2 (h2c hoặc TLS) và ánh xạ sang UDP DATA.
type Server struct {
	cfg     Config
	tx      Requester
	log     *zap.Logger
	metrics *metrics.Metrics
	http    *http.Server
	rps     *rpsGate

	mu sync.Mutex
	ln net.Listener
}

// New tạo HTTP server. Listen khi gọi Serve.
func New(cfg Config, tx Requester, log *zap.Logger, met *metrics.Metrics) (*Server, error) {
	if tx == nil {
		return nil, fmt.Errorf("http requester is required")
	}
	if cfg.NodeID == 0 {
		return nil, fmt.Errorf("http node_id must be > 0")
	}
	if log == nil {
		log = logger.OrNop(nil)
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 15 * time.Second
	}
	if met == nil {
		met = metrics.New()
	}
	s := &Server{cfg: cfg, tx: tx, log: log, metrics: met, rps: newRPSGate(cfg.MaxRPS, cfg.MaxRPSBurst, met)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.Handle("GET /metrics", met.Handler())
	mux.HandleFunc("GET /metrics.json", s.handleMetricsJSON)
	mux.HandleFunc("POST /v1/data", s.handleData)
	// F2: nới stream/conn (default http2 ~250) để khớp tải client cao.
	h2s := &http2.Server{
		MaxConcurrentStreams: 1024,
	}
	s.http = &http.Server{
		Addr:              net.JoinHostPort(cfg.Listen, strconv.Itoa(cfg.Port)),
		Handler:           h2c.NewHandler(mux, h2s),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
	}
	return s, nil
}

// Addr trả về địa chỉ đang listen (sau Serve hoặc Listen).
func (s *Server) Addr() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Serve lắng nghe và phục vụ đến khi Shutdown hoặc lỗi.
func (s *Server) Serve() error {
	if s == nil || s.http == nil {
		return fmt.Errorf("http server is nil")
	}
	ln, err := net.Listen("tcp", s.http.Addr)
	if err != nil {
		return fmt.Errorf("listen http: %w", err)
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	s.rps.start(context.Background())
	s.log.Info("http2gw đang lắng nghe HTTP",
		zap.String("addr", ln.Addr().String()),
		zap.Bool("tls", s.cfg.TLSCert != ""),
		zap.Float64("max_rps", s.cfg.MaxRPS),
		zap.Int("max_rps_burst", s.cfg.MaxRPSBurst),
	)
	if s.cfg.TLSCert != "" {
		return s.http.ServeTLS(ln, s.cfg.TLSCert, s.cfg.TLSKey)
	}
	return s.http.Serve(ln)
}

// Shutdown dừng nhận request mới.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.http == nil {
		return nil
	}
	s.rps.close()
	return s.http.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

func (s *Server) handleMetricsJSON(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// Pretty JSON + envelope giống zap (level/msg) — dễ đọc khi curl.
	type view struct {
		Level string `json:"level"`
		Msg   string `json:"msg"`
		metrics.RequestStats
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(view{
		Level:        "info",
		Msg:          "metrics_snapshot",
		RequestStats: s.metrics.Snapshot(),
	})
}

func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	s.rps.observeArrival()
	if !s.rps.allow() {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		s.metrics.Observe(time.Since(start), http.StatusTooManyRequests, metrics.ReasonRateLimited)
		return
	}
	if s.cfg.MaxPending > 0 && s.metrics != nil {
		if pend := s.metrics.GetManagerPending(); pend > int64(s.cfg.MaxPending) {
			http.Error(w, "too many pending transactions", http.StatusTooManyRequests)
			s.metrics.AddHTTPRateLimited(1)
			s.metrics.Observe(time.Since(start), http.StatusTooManyRequests, metrics.ReasonRateLimited)
			return
		}
	}
	s.metrics.AddInFlight(1)
	status, reason := s.serveData(w, r)
	s.metrics.AddInFlight(-1)
	s.metrics.Observe(time.Since(start), status, reason)
	s.log.Debug("http data done",
		zap.Int("status", status),
		zap.String("reason", reason),
		zap.Duration("latency", time.Since(start)),
		zap.String("proto", r.Proto),
	)
}

func (s *Server) serveData(w http.ResponseWriter, r *http.Request) (int, string) {
	msgID, err := parseUint32Header(r, "X-Message-Id")
	if err != nil || msgID == 0 {
		s.log.Debug("http data reject", zap.String("reason", metrics.ReasonMissingMessageID))
		http.Error(w, "X-Message-Id is required", http.StatusBadRequest)
		return http.StatusBadRequest, metrics.ReasonMissingMessageID
	}
	body, bodyBuf, err := readBodyPooled(r.Body, maxBody+1)
	if err != nil {
		s.log.Debug("http data reject", zap.String("reason", metrics.ReasonInvalidBody), zap.Error(err))
		http.Error(w, "read body", http.StatusBadRequest)
		return http.StatusBadRequest, metrics.ReasonInvalidBody
	}
	defer releaseBodyBuf(bodyBuf)
	if len(body) > maxBody {
		s.log.Debug("http data reject", zap.String("reason", metrics.ReasonBodyTooLarge), zap.Int("body_bytes", len(body)))
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return http.StatusRequestEntityTooLarge, metrics.ReasonBodyTooLarge
	}

	s.log.Debug("http data in",
		zap.String("proto", r.Proto),
		zap.Uint32("message_id", msgID),
		zap.String("session_id", strings.TrimSpace(r.Header.Get("X-Session-Id"))),
		zap.String("trace_id", strings.TrimSpace(r.Header.Get("X-Trace-Id"))),
		zap.Int("body_bytes", len(body)),
		zap.ByteString("body", body),
	)

	ts := time.Now().UnixMilli()
	if ts < 0 {
		ts = 0
	}
	slot := acquireEnvSlot()
	defer releaseEnvSlot(slot)
	slot.env.Version = 1
	slot.env.Type = pb.MessageType_MESSAGE_TYPE_DATA_REQUEST
	slot.env.SourceNodeId = s.cfg.NodeID
	slot.env.TimestampMs = uint64(ts)
	slot.env.TraceId = strings.TrimSpace(r.Header.Get("X-Trace-Id"))
	slot.dr.MessageId = msgID
	slot.dr.SessionId = strings.TrimSpace(r.Header.Get("X-Session-Id"))
	slot.dr.Payload = body
	env := &slot.env

	resp, err := s.tx.Request(r.Context(), env)
	if resp != nil {
		defer udp.ReleaseEnvelope(resp)
	}
	if err != nil {
		s.log.Debug("http data tx thất bại",
			zap.Uint32("message_id", msgID),
			zap.Uint64("transaction_id", env.GetTransactionId()),
			zap.Error(err),
		)
		return writeTxError(w, err, env.GetTransactionId(), s.metrics)
	}
	data := resp.GetDataResponse()
	if data == nil {
		s.log.Debug("http data empty response", zap.Uint64("transaction_id", resp.GetTransactionId()))
		setTransactionIDHeader(w, resp.GetTransactionId())
		http.Error(w, "empty data response", http.StatusBadGateway)
		return http.StatusBadGateway, metrics.ReasonEmptyResponse
	}
	w.Header().Set("X-Transaction-Id", strconv.FormatUint(resp.GetTransactionId(), 10))
	w.Header().Set("X-Message-Id", strconv.FormatUint(uint64(data.GetMessageId()), 10))
	status := int(data.GetStatus())
	if status < 100 || status > 599 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if len(data.GetPayload()) > 0 {
		_, _ = w.Write(data.GetPayload())
	}
	s.log.Debug("http data out",
		zap.Uint64("transaction_id", resp.GetTransactionId()),
		zap.Uint32("message_id", data.GetMessageId()),
		zap.Int("status", status),
		zap.Int("payload_bytes", len(data.GetPayload())),
		zap.ByteString("payload", data.GetPayload()),
	)
	if status >= 400 {
		return status, metrics.ReasonLogicError
	}
	return status, metrics.ReasonOK
}

func setTransactionIDHeader(w http.ResponseWriter, txID uint64) {
	if txID == 0 {
		return
	}
	w.Header().Set("X-Transaction-Id", strconv.FormatUint(txID, 10))
}

func writeTxError(w http.ResponseWriter, err error, txID uint64, met *metrics.Metrics) (int, string) {
	setTransactionIDHeader(w, txID)
	var logicErr *transaction.LogicError
	if errors.As(err, &logicErr) && logicErr != nil {
		status, reason := mapLogicError(logicErr)
		http.Error(w, logicErr.Error(), status)
		return status, reason
	}
	switch {
	case errors.Is(err, transaction.ErrTransactionTimeout), errors.Is(err, context.DeadlineExceeded):
		if met != nil {
			met.AddHTTP504(1)
			met.AddTransactionTimeout(1)
			if errors.Is(err, transaction.ErrTransactionTimeout) {
				met.AddTimeout(1)
			} else {
				met.AddContextDeadline(1)
			}
		}
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return http.StatusGatewayTimeout, metrics.ReasonTimeout
	case errors.Is(err, router.ErrNoEligibleNode), errors.Is(err, router.ErrEmptyCandidates):
		met.AddRouteFailed(1)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return http.StatusServiceUnavailable, metrics.ReasonNoRoutingTarget
	case errors.Is(err, transaction.ErrInvalidNode):
		http.Error(w, err.Error(), http.StatusBadRequest)
		return http.StatusBadRequest, metrics.ReasonInvalidNode
	case errors.Is(err, transaction.ErrInvalidRequest), errors.Is(err, router.ErrUnsupportedMessageType):
		http.Error(w, err.Error(), http.StatusBadRequest)
		return http.StatusBadRequest, metrics.ReasonInvalidRequest
	case errors.Is(err, transaction.ErrManagerClosed):
		http.Error(w, err.Error(), http.StatusBadGateway)
		return http.StatusBadGateway, metrics.ReasonManagerClosed
	case errors.Is(err, context.Canceled):
		http.Error(w, err.Error(), 499)
		return 499, metrics.ReasonCanceled
	case errors.Is(err, udp.ErrTransportClosed), errors.Is(err, udp.ErrPacketTooLarge), errors.Is(err, udp.ErrInvalidMessage):
		http.Error(w, err.Error(), http.StatusBadGateway)
		return http.StatusBadGateway, metrics.ReasonUDPSendFailed
	default:
		http.Error(w, err.Error(), http.StatusBadGateway)
		return http.StatusBadGateway, metrics.ReasonUnknown
	}
}

// mapLogicError: Envelope ERROR → HTTP (tránh 504 giả khi Logic đã trả lỗi).
func mapLogicError(e *transaction.LogicError) (int, string) {
	if e == nil {
		return http.StatusBadGateway, metrics.ReasonLogicError
	}
	switch e.Code {
	case pb.ErrorCode_ERROR_CODE_TIMEOUT,
		pb.ErrorCode_ERROR_CODE_LOGIC_TIMEOUT,
		pb.ErrorCode_ERROR_CODE_HEARTBEAT_TIMEOUT:
		return http.StatusGatewayTimeout, metrics.ReasonTimeout
	case pb.ErrorCode_ERROR_CODE_INVALID_MESSAGE:
		return http.StatusBadRequest, metrics.ReasonInvalidRequest
	case pb.ErrorCode_ERROR_CODE_NODE_NOT_AVAILABLE,
		pb.ErrorCode_ERROR_CODE_OVERLOAD,
		pb.ErrorCode_ERROR_CODE_NO_ROUTING_TARGET:
		return http.StatusServiceUnavailable, metrics.ReasonNoRoutingTarget
	case pb.ErrorCode_ERROR_CODE_UDP_SEND_FAILED:
		return http.StatusBadGateway, metrics.ReasonUDPSendFailed
	default:
		return http.StatusBadGateway, metrics.ReasonLogicError
	}
}

func parseUint32Header(r *http.Request, name string) (uint32, error) {
	raw := strings.TrimSpace(r.Header.Get(name))
	if raw == "" {
		return 0, fmt.Errorf("missing %s", name)
	}
	v, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}
