package metrics

import (
	"strconv"
	"sync"
	"time"
)

// Mã nguyên nhân fail (ổn định để giám sát, không đổi tùy ý).
const (
	ReasonOK               = "ok"
	ReasonMissingMessageID = "missing_message_id" // HTTP 400
	ReasonInvalidBody      = "invalid_body"       // HTTP 400
	ReasonBodyTooLarge     = "body_too_large"     // HTTP 413
	ReasonInvalidRequest   = "invalid_request"    // HTTP 400
	ReasonInvalidNode      = "invalid_node"       // HTTP 400
	ReasonCanceled         = "canceled"           // HTTP 499
	ReasonEmptyResponse    = "empty_response"     // HTTP 502
	ReasonUDPSendFailed    = "udp_send_failed"    // HTTP 502
	ReasonManagerClosed    = "manager_closed"     // HTTP 502
	ReasonNoRoutingTarget  = "no_routing_target"  // HTTP 503
	ReasonTimeout          = "timeout"            // HTTP 504
	ReasonLogicError       = "logic_error"        // HTTP 4xx/5xx từ DataResponse.status
	ReasonUnknown          = "unknown"
)

var knownFailStatus = []int{400, 413, 499, 502, 503, 504}

var knownFailReasons = []string{
	ReasonMissingMessageID,
	ReasonInvalidBody,
	ReasonBodyTooLarge,
	ReasonInvalidRequest,
	ReasonInvalidNode,
	ReasonCanceled,
	ReasonEmptyResponse,
	ReasonUDPSendFailed,
	ReasonManagerClosed,
	ReasonNoRoutingTarget,
	ReasonTimeout,
	ReasonLogicError,
	ReasonUnknown,
}

// RequestStats là snapshot đọc được của xử lý POST /v1/data.
type RequestStats struct {
	RequestsTotal int64            `json:"requests_total"`
	RequestsOK    int64            `json:"requests_ok"`
	RequestsFail  int64            `json:"requests_fail"`
	InFlight      int64            `json:"in_flight"`
	ByStatus      map[string]int64 `json:"by_status"`
	FailByCode    map[string]int64 `json:"fail_by_code"`
	FailByReason  map[string]int64 `json:"fail_by_reason"`
	Latency       LatencyStats     `json:"latency"`

	LogicRegisteredTotal    int64 `json:"logic_registered_total"`
	HeartbeatSuccessTotal   int64 `json:"heartbeat_success_total"`
	HeartbeatTimeoutTotal   int64 `json:"heartbeat_timeout_total"`
	UDPRXTotal              int64 `json:"udp_rx_total"`
	UDPTXTotal              int64 `json:"udp_tx_total"`
	RouteFailedTotal        int64 `json:"route_failed_total"`
	TransactionTimeoutTotal int64 `json:"transaction_timeout_total"`
}

// LatencyStats thời gian phản hồi (từ vào handleData đến lúc ghi HTTP xong).
type LatencyStats struct {
	Count int64   `json:"count"`
	MinMS float64 `json:"min_ms"`
	MaxMS float64 `json:"max_ms"`
	AvgMS float64 `json:"avg_ms"`
	SumMS float64 `json:"sum_ms"`
}

// Metrics đếm request http2gw, an toàn khi nhiều goroutine.
type Metrics struct {
	mu           sync.Mutex
	total        int64
	ok           int64
	fail         int64
	inFlight     int64
	sum          time.Duration
	min          time.Duration
	max          time.Duration
	byStatus     map[int]int64
	failByCode   map[int]int64
	failByReason map[string]int64

	logicRegisteredTotal    int64
	heartbeatSuccessTotal   int64
	heartbeatTimeoutTotal   int64
	udpRxTotal              int64
	udpTxTotal              int64
	routeFailedTotal        int64
	transactionTimeoutTotal int64
}

// New tạo bộ đếm rỗng.
func New() *Metrics {
	return &Metrics{
		byStatus:     make(map[int]int64),
		failByCode:   make(map[int]int64),
		failByReason: make(map[string]int64),
	}
}

// AddInFlight +1 khi bắt đầu xử lý, -1 khi xong.
func (m *Metrics) AddInFlight(delta int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.inFlight += delta
	if m.inFlight < 0 {
		m.inFlight = 0
	}
	m.mu.Unlock()
}

// Observe ghi nhận một request đã hoàn tất.
// reason dùng hằng Reason* ; thành công thì ReasonOK hoặc "".
func (m *Metrics) Observe(d time.Duration, status int, reason string) {
	if m == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.total++
	m.sum += d
	if m.min == 0 || d < m.min {
		m.min = d
	}
	if d > m.max {
		m.max = d
	}
	if m.byStatus == nil {
		m.byStatus = make(map[int]int64)
	}
	m.byStatus[status]++
	if status >= 200 && status < 400 {
		m.ok++
		return
	}
	m.fail++
	if m.failByCode == nil {
		m.failByCode = make(map[int]int64)
	}
	m.failByCode[status]++
	if reason == "" || reason == ReasonOK {
		reason = ReasonUnknown
	}
	if m.failByReason == nil {
		m.failByReason = make(map[string]int64)
	}
	m.failByReason[reason]++
}

func (m *Metrics) AddLogicRegistered(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.logicRegisteredTotal += n
	m.mu.Unlock()
}

func (m *Metrics) AddHeartbeatSuccess(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.heartbeatSuccessTotal += n
	m.mu.Unlock()
}

func (m *Metrics) AddHeartbeatTimeout(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.heartbeatTimeoutTotal += n
	m.mu.Unlock()
}

func (m *Metrics) AddUDPRx(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.udpRxTotal += n
	m.mu.Unlock()
}

func (m *Metrics) AddUDPTx(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.udpTxTotal += n
	m.mu.Unlock()
}

func (m *Metrics) AddRouteFailed(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.routeFailedTotal += n
	m.mu.Unlock()
}

func (m *Metrics) AddTransactionTimeout(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.transactionTimeoutTotal += n
	m.mu.Unlock()
}

// Snapshot copy số liệu hiện tại. fail_by_code / fail_by_reason luôn có đủ khóa đã biết (0 nếu chưa xảy ra).
func (m *Metrics) Snapshot() RequestStats {
	if m == nil {
		return emptyStats()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	by := make(map[string]int64, len(m.byStatus))
	for code, n := range m.byStatus {
		by[strconv.Itoa(code)] = n
	}
	failCode := make(map[string]int64, len(knownFailStatus)+len(m.failByCode))
	for _, code := range knownFailStatus {
		failCode[strconv.Itoa(code)] = m.failByCode[code]
	}
	for code, n := range m.failByCode {
		failCode[strconv.Itoa(code)] = n
	}
	failReason := make(map[string]int64, len(knownFailReasons)+len(m.failByReason))
	for _, r := range knownFailReasons {
		failReason[r] = m.failByReason[r]
	}
	for r, n := range m.failByReason {
		failReason[r] = n
	}
	st := RequestStats{
		RequestsTotal: m.total,
		RequestsOK:    m.ok,
		RequestsFail:  m.fail,
		InFlight:      m.inFlight,
		ByStatus:      by,
		FailByCode:    failCode,
		FailByReason:  failReason,
		Latency: LatencyStats{
			Count: m.total,
			MinMS: durationMS(m.min),
			MaxMS: durationMS(m.max),
			SumMS: durationMS(m.sum),
		},
		LogicRegisteredTotal:    m.logicRegisteredTotal,
		HeartbeatSuccessTotal:   m.heartbeatSuccessTotal,
		HeartbeatTimeoutTotal:   m.heartbeatTimeoutTotal,
		UDPRXTotal:              m.udpRxTotal,
		UDPTXTotal:              m.udpTxTotal,
		RouteFailedTotal:        m.routeFailedTotal,
		TransactionTimeoutTotal: m.transactionTimeoutTotal,
	}
	if m.total > 0 {
		st.Latency.AvgMS = durationMS(m.sum / time.Duration(m.total))
	}
	return st
}

func emptyStats() RequestStats {
	m := New()
	return m.Snapshot()
}

func durationMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
