package logicnode

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const logicMetricsNamespace = "logic"

var logicDiagBuckets = []float64{0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// nodeMetrics Prometheus instrumentation cho process Logic.
type nodeMetrics struct {
	registry *prometheus.Registry

	requests prometheus.Counter // legacy: handled inside worker

	udpDataRequestRX  prometheus.Counter
	udpDataResponseTX prometheus.Counter
	udpReceiveError   prometheus.Counter
	udpSendError      prometheus.Counter

	poolCapacity     prometheus.Gauge
	poolInUse        prometheus.Gauge
	poolWaitTotal    prometheus.Counter
	poolWaitDuration prometheus.Histogram
	poolBlockedTotal prometheus.Counter // legacy: receiveLoop blocked (giữ tương thích scrape)

	jobQueueCapacity prometheus.Gauge
	jobQueueDepth    prometheus.Gauge
	jobQueueDropped  prometheus.Counter

	processingDuration prometheus.Histogram

	// Design §17 Logic metrics.
	registerStatus prometheus.Gauge // 0 = chưa accepted, 1 = REGISTER accepted
	heartbeatRTT   prometheus.Gauge // giây — RTT HB gần nhất (Logic→GW→Logic)
	queueSize      prometheus.Gauge // in-flight DATA đang xử lý trong worker

	poolSatLogNanos atomic.Int64
}

func newNodeMetrics(nodeID uint32, nodeName string) *nodeMetrics {
	name := nodeName
	if name == "" {
		name = strconv.FormatUint(uint64(nodeID), 10)
	}
	labels := prometheus.Labels{
		"node_id": strconv.FormatUint(uint64(nodeID), 10),
		"node":    name,
	}
	reg := prometheus.NewRegistry()
	m := &nodeMetrics{
		registry: reg,
		requests: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "requests_total",
			Help:        "DATA_REQUEST handled by worker (after pool admit).",
			ConstLabels: labels,
		}),
		udpDataRequestRX: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "udp_data_request_rx_total",
			Help:        "DATA_REQUEST datagrams read from UDP before worker pool.",
			ConstLabels: labels,
		}),
		udpDataResponseTX: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "udp_data_response_tx_total",
			Help:        "DATA_RESPONSE/ERROR datagrams successfully sent to gateway.",
			ConstLabels: labels,
		}),
		udpReceiveError: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "udp_receive_error_total",
			Help:        "UDP receive errors (invalid envelope or read failure).",
			ConstLabels: labels,
		}),
		udpSendError: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "udp_send_error_total",
			Help:        "UDP send errors when replying DATA/ERROR/HEARTBEAT.",
			ConstLabels: labels,
		}),
		poolCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "pool_capacity",
			Help:        "DATA worker pool semaphore capacity.",
			ConstLabels: labels,
		}),
		poolInUse: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "pool_in_use",
			Help:        "DATA worker pool slots currently in use.",
			ConstLabels: labels,
		}),
		poolWaitTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "pool_wait_total",
			Help:        "Times receiveLoop waited on pool semaphore (pool was full).",
			ConstLabels: labels,
		}),
		poolWaitDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "pool_wait_duration_seconds",
			Help:        "Time receiveLoop blocked acquiring a worker pool slot.",
			ConstLabels: labels,
			Buckets:     logicDiagBuckets,
		}),
		poolBlockedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "pool_receive_blocked_total",
			Help:        "Legacy: receiveLoop blocked on pool (should stay ~0 after non-blocking queue).",
			ConstLabels: labels,
		}),
		jobQueueCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "job_queue_capacity",
			Help:        "Bounded DATA job queue capacity (non-blocking enqueue).",
			ConstLabels: labels,
		}),
		jobQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "job_queue_depth",
			Help:        "Current DATA job queue depth.",
			ConstLabels: labels,
		}),
		jobQueueDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "job_queue_dropped_total",
			Help:        "DATA_REQUEST dropped because job queue was full (receiveLoop not blocked).",
			ConstLabels: labels,
		}),
		processingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "processing_duration_seconds",
			Help:        "Logic DATA handleBusiness + Send duration inside worker.",
			ConstLabels: labels,
			Buckets:     logicDiagBuckets,
		}),
		registerStatus: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "register_status",
			Help:        "1 if REGISTER accepted by HTTP2GW, else 0 (design §17).",
			ConstLabels: labels,
		}),
		heartbeatRTT: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "heartbeat_rtt_seconds",
			Help:        "Latest Logic→GW heartbeat RTT in seconds (design §17 heartbeat_rtt).",
			ConstLabels: labels,
		}),
		queueSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   logicMetricsNamespace,
			Name:        "queue_size",
			Help:        "In-flight DATA requests being processed (design §17 queue_size).",
			ConstLabels: labels,
		}),
	}
	reg.MustRegister(
		m.requests,
		m.udpDataRequestRX,
		m.udpDataResponseTX,
		m.udpReceiveError,
		m.udpSendError,
		m.poolCapacity,
		m.poolInUse,
		m.poolWaitTotal,
		m.poolWaitDuration,
		m.poolBlockedTotal,
		m.jobQueueCapacity,
		m.jobQueueDepth,
		m.jobQueueDropped,
		m.processingDuration,
		m.registerStatus,
		m.heartbeatRTT,
		m.queueSize,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

func (m *nodeMetrics) AddRequest() {
	if m == nil {
		return
	}
	m.requests.Inc()
}

func (m *nodeMetrics) AddUDPDataRequestRX() {
	if m == nil {
		return
	}
	m.udpDataRequestRX.Inc()
}

func (m *nodeMetrics) AddUDPDataResponseTX() {
	if m == nil {
		return
	}
	m.udpDataResponseTX.Inc()
}

func (m *nodeMetrics) AddUDPReceiveError() {
	if m == nil {
		return
	}
	m.udpReceiveError.Inc()
}

func (m *nodeMetrics) AddUDPSendError() {
	if m == nil {
		return
	}
	m.udpSendError.Inc()
}

func (m *nodeMetrics) SetPoolCapacity(n int) {
	if m == nil {
		return
	}
	m.poolCapacity.Set(float64(n))
}

func (m *nodeMetrics) SetPoolInUse(n int) {
	if m == nil {
		return
	}
	m.poolInUse.Set(float64(n))
}

func (m *nodeMetrics) ObservePoolWait(d time.Duration, blocked bool) {
	if m == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	m.poolWaitDuration.Observe(d.Seconds())
	if blocked {
		m.poolWaitTotal.Inc()
		m.poolBlockedTotal.Inc()
	}
}

func (m *nodeMetrics) SetJobQueueCapacity(n int) {
	if m == nil {
		return
	}
	m.jobQueueCapacity.Set(float64(n))
}

func (m *nodeMetrics) SetJobQueueDepth(n int) {
	if m == nil {
		return
	}
	if n < 0 {
		n = 0
	}
	m.jobQueueDepth.Set(float64(n))
}

func (m *nodeMetrics) AddJobQueueDropped(n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.jobQueueDropped.Add(float64(n))
}

func (m *nodeMetrics) ObserveProcessing(d time.Duration) {
	if m == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	m.processingDuration.Observe(d.Seconds())
}

// SetRegisterStatus: 1 = REGISTER accepted, 0 = chưa / mất đăng ký.
func (m *nodeMetrics) SetRegisterStatus(accepted bool) {
	if m == nil {
		return
	}
	if accepted {
		m.registerStatus.Set(1)
		return
	}
	m.registerStatus.Set(0)
}

// SetHeartbeatRTT ghi RTT heartbeat gần nhất (giây).
func (m *nodeMetrics) SetHeartbeatRTT(d time.Duration) {
	if m == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	m.heartbeatRTT.Set(d.Seconds())
}

// SetQueueSize số DATA in-flight (đang xử lý trong worker).
func (m *nodeMetrics) SetQueueSize(n int64) {
	if m == nil {
		return
	}
	if n < 0 {
		n = 0
	}
	m.queueSize.Set(float64(n))
}

func (m *nodeMetrics) shouldLogPoolSaturation() bool {
	if m == nil {
		return false
	}
	now := time.Now().UnixNano()
	prev := m.poolSatLogNanos.Load()
	if now-prev < int64(time.Second) {
		return false
	}
	return m.poolSatLogNanos.CompareAndSwap(prev, now)
}

func (m *nodeMetrics) Handler() http.Handler {
	if m == nil || m.registry == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
		})
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// startMetricsServer lắng HTTP /metrics. port <= 0 thì no-op.
func startMetricsServer(ctx context.Context, listen string, port int, met *nodeMetrics, log *zap.Logger) error {
	if port <= 0 || met == nil {
		return nil
	}
	if listen == "" {
		listen = "0.0.0.0"
	}
	addr := net.JoinHostPort(listen, strconv.Itoa(port))
	mux := http.NewServeMux()
	mux.Handle("/metrics", met.Handler())
	// MetricServer h2c — cùng kiểu: h2c.NewHandler(router, &http2.Server{}).
	srv := &http.Server{
		Addr:              addr,
		Handler:           h2c.NewHandler(mux, &http2.Server{}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics listen %s: %w", addr, err)
	}
	log.Info("logic MetricServer (h2c) đang lắng nghe",
		zap.String("addr", ln.Addr().String()),
		zap.String("path", "/metrics"),
		zap.Strings("diag", []string{
			"register_status",
			"heartbeat_rtt_seconds",
			"queue_size",
			"udp_data_request_rx_total",
			"udp_data_response_tx_total",
			"pool_wait_total",
			"pool_receive_blocked_total",
			"processing_duration_seconds",
		}),
	)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		err := srv.Serve(ln)
		if err != nil && err != http.ErrServerClosed {
			log.Warn("logic metrics server dừng", zap.Error(err))
		}
	}()
	return nil
}
