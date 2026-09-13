package metrics

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	ReasonRateLimited      = "rate_limited"       // HTTP 429
	ReasonLogicError       = "logic_error"        // HTTP 4xx/5xx từ DataResponse.status
	ReasonUnknown          = "unknown"
)

var knownFailStatus = []int{400, 413, 429, 499, 502, 503, 504}

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
	ReasonRateLimited,
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
	// LogicRoutedByNode: số DATA_REQUEST đã gửi thành công tới từng Logic (key = node label).
	LogicRoutedByNode map[string]int64 `json:"logic_routed_by_node"`

	ManagerCreatedTotal          int64 `json:"manager_created_total"`
	ManagerCompletedTotal        int64 `json:"manager_completed_total"`
	ManagerTimeoutTotal          int64 `json:"manager_timeout_total"`
	ManagerCompleteNotFoundTotal int64 `json:"manager_complete_not_found_total"`
	ManagerPending               int64 `json:"manager_pending"`
	GWUDPDataRequestTXTotal      int64 `json:"gw_udp_data_request_tx_total"`
	GWUDPDataResponseRXTotal     int64 `json:"gw_udp_data_response_rx_total"`
	GWUDPSendErrorTotal          int64 `json:"gw_udp_send_error_total"`
	GWUDPReceiveErrorTotal       int64 `json:"gw_udp_receive_error_total"`
	UDPResponseQueueDepth        int64 `json:"udp_response_queue_depth"`
	UDPResponseQueueFullTotal    int64 `json:"udp_response_queue_full_total"`
	UDPResponseDroppedTotal      int64 `json:"udp_response_dropped_total"`
	HTTP504Total                 int64 `json:"http_504_total"`
	TimeoutTotal                 int64 `json:"timeout_total"`
	ContextDeadlineTotal         int64 `json:"context_deadline_total"`

	HTTPReceivedTotal    int64   `json:"http_received_total"`
	HTTPRateLimitedTotal int64   `json:"http_rate_limited_total"`
	HTTPRPS              float64 `json:"http_rps"`
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
// Snapshot JSON và Prometheus exposition cùng được cập nhật (dual-write).
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
	logicRoutedByNode       map[string]int64

	managerCreatedTotal          int64
	managerCompletedTotal        int64
	managerTimeoutTotal          int64
	managerCompleteNotFoundTotal int64
	managerPending               atomic.Int64
	gwUDPDataRequestTXTotal      int64
	gwUDPDataResponseRXTotal     int64
	gwUDPSendErrorTotal          int64
	gwUDPReceiveErrorTotal       int64
	udpResponseQueueDepth        int64
	udpResponseQueueFullTotal    int64
	udpResponseDroppedTotal      int64
	http504Total                 int64
	timeoutTotal                 int64
	contextDeadlineTotal         int64
	httpReceivedTotal            int64
	httpRateLimitedTotal         int64
	httpRPS                      float64

	prom *promCollectors
}

// New tạo bộ đếm rỗng + registry Prometheus riêng (an toàn khi test song song).
func New() *Metrics {
	return &Metrics{
		byStatus:     make(map[int]int64),
		failByCode:   make(map[int]int64),
		failByReason: make(map[string]int64),
		prom:         newPromCollectors(),
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
	cur := m.inFlight
	m.mu.Unlock()
	if m.prom != nil {
		m.prom.inFlight.Set(float64(cur))
	}
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
	ok := status >= 200 && status < 400
	if ok {
		m.ok++
	} else {
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
	m.mu.Unlock()
	if !ok && (reason == "" || reason == ReasonOK) {
		reason = ReasonUnknown
	}
	m.observeProm(d.Seconds(), status, reason, ok)
}

func (m *Metrics) AddLogicRegistered(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.logicRegisteredTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.logicRegistered.Add(float64(n))
	}
}

func (m *Metrics) AddHeartbeatSuccess(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.heartbeatSuccessTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.heartbeatSuccess.Add(float64(n))
	}
}

func (m *Metrics) AddHeartbeatTimeout(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.heartbeatTimeoutTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.heartbeatTimeout.Add(float64(n))
	}
}

func (m *Metrics) AddUDPRx(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.udpRxTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.udpRx.Add(float64(n))
	}
}

func (m *Metrics) AddUDPTx(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.udpTxTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.udpTx.Add(float64(n))
	}
}

func (m *Metrics) AddRouteFailed(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.routeFailedTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.routeFailed.Add(float64(n))
	}
}

func (m *Metrics) AddTransactionTimeout(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.transactionTimeoutTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.transactionTimeout.Add(float64(n))
	}
}

// ObserveLogicRegistry cập nhật gauge số Logic theo state (DATA-plane registry).
func (m *Metrics) ObserveLogicRegistry(byState map[string]int) {
	if m == nil || m.prom == nil {
		return
	}
	for _, st := range []string{"registered", "active", "suspect", "dead", "init"} {
		m.prom.logicNodes.WithLabelValues(st).Set(float64(byState[st]))
	}
}

// AddFailoverRetry đếm lần failover retry (Send fail / timeout).
func (m *Metrics) AddFailoverRetry(n int64) {
	if m == nil || m.prom == nil || n <= 0 {
		return
	}
	m.prom.failoverRetryTotal.Add(float64(n))
}

// AddLogicRouted đếm DATA_REQUEST đã Route + Send UDP thành công tới một Logic.
// nodeName nên là Name hoặc InstanceID; rỗng thì dùng node_id.
func (m *Metrics) AddLogicRouted(nodeID uint32, nodeName string) {
	if m == nil {
		return
	}
	idLabel := strconv.FormatUint(uint64(nodeID), 10)
	name := strings.TrimSpace(nodeName)
	if name == "" {
		name = idLabel
	}
	m.mu.Lock()
	if m.logicRoutedByNode == nil {
		m.logicRoutedByNode = make(map[string]int64)
	}
	m.logicRoutedByNode[name]++
	m.mu.Unlock()
	if m.prom != nil {
		m.prom.logicRouted.WithLabelValues(idLabel, name).Inc()
	}
}

func (m *Metrics) AddManagerCreated(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.managerCreatedTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.managerCreated.Add(float64(n))
	}
}

func (m *Metrics) AddManagerCompleted(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.managerCompletedTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.managerCompleted.Add(float64(n))
	}
}

func (m *Metrics) AddManagerTimeout(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.managerTimeoutTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.managerTimeout.Add(float64(n))
	}
}

func (m *Metrics) AddManagerCompleteNotFound(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.managerCompleteNotFoundTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.managerCompleteNotFound.Add(float64(n))
	}
}

func (m *Metrics) SetManagerPending(n int64) {
	if m == nil {
		return
	}
	if n < 0 {
		n = 0
	}
	m.managerPending.Store(n)
	if m.prom != nil {
		m.prom.managerPending.Set(float64(n))
	}
}

// GetManagerPending số waiter đang mở (admission gate).
func (m *Metrics) GetManagerPending() int64 {
	if m == nil {
		return 0
	}
	return m.managerPending.Load()
}

func (m *Metrics) ObserveManagerWait(d time.Duration) {
	if m == nil || m.prom == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	m.prom.managerWaitDuration.Observe(d.Seconds())
}

func (m *Metrics) ObserveTransactionTotal(d time.Duration) {
	if m == nil || m.prom == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	m.prom.transactionTotalDuration.Observe(d.Seconds())
}

func (m *Metrics) AddGWUDPDataRequestTX(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.gwUDPDataRequestTXTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.gwUDPDataRequestTX.Add(float64(n))
	}
}

func (m *Metrics) AddGWUDPDataResponseRX(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.gwUDPDataResponseRXTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.gwUDPDataResponseRX.Add(float64(n))
	}
}

func (m *Metrics) AddGWUDPSendError(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.gwUDPSendErrorTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.gwUDPSendError.Add(float64(n))
	}
}

func (m *Metrics) AddGWUDPReceiveError(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.gwUDPReceiveErrorTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.gwUDPReceiveError.Add(float64(n))
	}
}

func (m *Metrics) SetUDPResponseQueueDepth(n int) {
	if m == nil {
		return
	}
	if n < 0 {
		n = 0
	}
	m.mu.Lock()
	m.udpResponseQueueDepth = int64(n)
	m.mu.Unlock()
	if m.prom != nil {
		m.prom.udpResponseQueueDepth.Set(float64(n))
	}
}

func (m *Metrics) AddUDPResponseQueueFull(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.udpResponseQueueFullTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.udpResponseQueueFull.Add(float64(n))
	}
}

func (m *Metrics) AddUDPResponseDropped(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.udpResponseDroppedTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.udpResponseDropped.Add(float64(n))
	}
}

func (m *Metrics) AddHTTP504(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.http504Total += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.http504Total.Add(float64(n))
	}
}

func (m *Metrics) AddTimeout(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.timeoutTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.timeoutTotal.Add(float64(n))
	}
}

func (m *Metrics) AddContextDeadline(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.contextDeadlineTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.contextDeadlineTotal.Add(float64(n))
	}
}

func (m *Metrics) AddHTTPReceived(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.httpReceivedTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.httpReceived.Add(float64(n))
	}
}

func (m *Metrics) AddHTTPRateLimited(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.mu.Lock()
	m.httpRateLimitedTotal += n
	m.mu.Unlock()
	if m.prom != nil && n > 0 {
		m.prom.httpRateLimited.Add(float64(n))
	}
}

func (m *Metrics) SetHTTPRPS(rps float64) {
	if m == nil {
		return
	}
	if rps < 0 {
		rps = 0
	}
	m.mu.Lock()
	m.httpRPS = rps
	m.mu.Unlock()
	if m.prom != nil {
		m.prom.httpRPS.Set(rps)
	}
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
	routed := make(map[string]int64, len(m.logicRoutedByNode))
	for k, v := range m.logicRoutedByNode {
		routed[k] = v
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
		RouteFailedTotal:             m.routeFailedTotal,
		TransactionTimeoutTotal:      m.transactionTimeoutTotal,
		LogicRoutedByNode:            routed,
		ManagerCreatedTotal:          m.managerCreatedTotal,
		ManagerCompletedTotal:        m.managerCompletedTotal,
		ManagerTimeoutTotal:          m.managerTimeoutTotal,
		ManagerCompleteNotFoundTotal: m.managerCompleteNotFoundTotal,
		ManagerPending:               m.managerPending.Load(),
		GWUDPDataRequestTXTotal:      m.gwUDPDataRequestTXTotal,
		GWUDPDataResponseRXTotal:     m.gwUDPDataResponseRXTotal,
		GWUDPSendErrorTotal:          m.gwUDPSendErrorTotal,
		GWUDPReceiveErrorTotal:       m.gwUDPReceiveErrorTotal,
		UDPResponseQueueDepth:        m.udpResponseQueueDepth,
		UDPResponseQueueFullTotal:    m.udpResponseQueueFullTotal,
		UDPResponseDroppedTotal:      m.udpResponseDroppedTotal,
		HTTP504Total:                 m.http504Total,
		TimeoutTotal:                 m.timeoutTotal,
		ContextDeadlineTotal:         m.contextDeadlineTotal,
		HTTPReceivedTotal:            m.httpReceivedTotal,
		HTTPRateLimitedTotal:         m.httpRateLimitedTotal,
		HTTPRPS:                      m.httpRPS,
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
