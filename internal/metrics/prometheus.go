package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "http2gw"

var diagBuckets = []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

type promCollectors struct {
	registry *prometheus.Registry

	requestsTotal   *prometheus.CounterVec
	requestsFail    *prometheus.CounterVec
	inFlight        prometheus.Gauge
	requestDuration prometheus.Histogram

	logicRegistered    prometheus.Counter
	heartbeatSuccess   prometheus.Counter
	heartbeatTimeout   prometheus.Counter
	udpRx              prometheus.Counter
	udpTx              prometheus.Counter
	routeFailed        prometheus.Counter
	transactionTimeout prometheus.Counter
	logicNodes         *prometheus.GaugeVec
	failoverRetryTotal prometheus.Counter
	logicRouted        *prometheus.CounterVec

	// Manager / 504 / UDP DATA diagnosis (Phase instrumentation).
	managerCreated           prometheus.Counter
	managerCompleted         prometheus.Counter
	managerTimeout           prometheus.Counter
	managerCompleteNotFound  prometheus.Counter
	managerPending           prometheus.Gauge
	managerWaitDuration      prometheus.Histogram
	transactionTotalDuration prometheus.Histogram

	gwUDPDataRequestTX  prometheus.Counter
	gwUDPDataResponseRX prometheus.Counter
	gwUDPSendError      prometheus.Counter
	gwUDPReceiveError   prometheus.Counter

	udpResponseQueueDepth prometheus.Gauge
	udpResponseQueueFull  prometheus.Counter
	udpResponseDropped    prometheus.Counter

	http504Total         prometheus.Counter
	timeoutTotal         prometheus.Counter
	contextDeadlineTotal prometheus.Counter

	httpReceived    prometheus.Counter
	httpRateLimited prometheus.Counter
	httpRPS         prometheus.Gauge
}

func newPromCollectors() *promCollectors {
	reg := prometheus.NewRegistry()
	p := &promCollectors{
		registry: reg,
		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "http_requests_total",
			Help:      "Total POST /v1/data requests completed.",
		}, []string{"status", "result"}),
		requestsFail: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "http_requests_fail_total",
			Help:      "Failed POST /v1/data requests by HTTP status and reason.",
		}, []string{"status", "reason"}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "http_requests_in_flight",
			Help:      "POST /v1/data requests currently being handled.",
		}),
		requestDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_request_duration_seconds",
			Help:      "Latency of POST /v1/data from accept to response write.",
			Buckets:   diagBuckets,
		}),
		logicRegistered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "logic_registered_total",
			Help:      "Accepted Logic REGISTER requests.",
		}),
		heartbeatSuccess: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "heartbeat_success_total",
			Help:      "Successful Logic→gateway HEARTBEAT_REQUEST handles.",
		}),
		heartbeatTimeout: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "heartbeat_timeout_total",
			Help:      "Heartbeat monitor timeout events (SUSPECT/DEAD transitions counted by monitor).",
		}),
		udpRx: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "udp_rx_total",
			Help:      "UDP envelopes received by dispatcher.",
		}),
		udpTx: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "udp_tx_total",
			Help:      "UDP envelopes sent by dispatcher (register/heartbeat replies).",
		}),
		routeFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "route_failed_total",
			Help:      "Requests that failed routing (no eligible Logic).",
		}),
		transactionTimeout: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "transaction_timeout_total",
			Help:      "HTTP responses mapped from transaction timeout / deadline (final 504 path).",
		}),
		logicNodes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "logic_nodes",
			Help:      "Count of Logic nodes in DATA-plane registry by state.",
		}, []string{"state"}),
		failoverRetryTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "failover_retry_total",
			Help:      "Transaction failover retries after send fail or timeout.",
		}),
		logicRouted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "logic_routed_total",
			Help:      "DATA_REQUEST successfully sent to a Logic node after routing.",
		}, []string{"node_id", "node"}),

		managerCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "manager_created_total",
			Help:      "Transaction waiters created in Manager.",
		}),
		managerCompleted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "manager_completed_total",
			Help:      "Successful Manager.Complete deliveries.",
		}),
		managerTimeout: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "manager_timeout_total",
			Help:      "Manager.Wait ended with context deadline exceeded (includes attempt/failover waits).",
		}),
		managerCompleteNotFound: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "manager_complete_not_found_total",
			Help:      "DATA_RESPONSE Complete called but no matching pending transaction.",
		}),
		managerPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "manager_pending",
			Help:      "Current pending transactions waiting for DATA_RESPONSE.",
		}),
		managerWaitDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "manager_wait_duration_seconds",
			Help:      "Duration of Manager.Wait (success or timeout/cancel).",
			Buckets:   diagBuckets,
		}),
		transactionTotalDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "transaction_total_duration_seconds",
			Help:      "Duration of transaction.Service.Request end-to-end.",
			Buckets:   diagBuckets,
		}),

		gwUDPDataRequestTX: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "gw_udp_data_request_tx_total",
			Help:      "DATA_REQUEST datagrams successfully written by gateway to Logic.",
		}),
		gwUDPDataResponseRX: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "gw_udp_data_response_rx_total",
			Help:      "DATA_RESPONSE envelopes received by gateway dispatcher.",
		}),
		gwUDPSendError: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "gw_udp_send_error_total",
			Help:      "UDP send errors on gateway DATA_REQUEST path.",
		}),
		gwUDPReceiveError: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "gw_udp_receive_error_total",
			Help:      "UDP receive errors on gateway dispatcher (invalid envelope or read error).",
		}),

		udpResponseQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "udp_response_queue_depth",
			Help:      "Current depth of gateway UDP inbound dispatch queue (all envelope types).",
		}),
		udpResponseQueueFull: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "udp_response_queue_full_total",
			Help:      "Times an inbound UDP envelope could not be enqueued (bounded queue full).",
		}),
		udpResponseDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "udp_response_dropped_total",
			Help:      "Inbound UDP envelopes dropped after socket read because the dispatch queue was full.",
		}),

		http504Total: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "http_504_total",
			Help:      "HTTP 504 responses written for POST /v1/data.",
		}),
		timeoutTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "timeout_total",
			Help:      "504 path attributed to ErrTransactionTimeout.",
		}),
		contextDeadlineTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "context_deadline_total",
			Help:      "504 path attributed to context.DeadlineExceeded (without ErrTransactionTimeout).",
		}),
		httpReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "http_received_total",
			Help:      "POST /v1/data requests received (counted at handler entry, before admission).",
		}),
		httpRateLimited: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "http_rate_limited_total",
			Help:      "POST /v1/data rejected by max_rps admission (HTTP 429).",
		}),
		httpRPS: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "http_rps",
			Help:      "Requests received in the previous 1-second window (updated every second).",
		}),
	}
	reg.MustRegister(
		p.requestsTotal,
		p.requestsFail,
		p.inFlight,
		p.requestDuration,
		p.logicRegistered,
		p.heartbeatSuccess,
		p.heartbeatTimeout,
		p.udpRx,
		p.udpTx,
		p.routeFailed,
		p.transactionTimeout,
		p.logicNodes,
		p.failoverRetryTotal,
		p.logicRouted,
		p.managerCreated,
		p.managerCompleted,
		p.managerTimeout,
		p.managerCompleteNotFound,
		p.managerPending,
		p.managerWaitDuration,
		p.transactionTotalDuration,
		p.gwUDPDataRequestTX,
		p.gwUDPDataResponseRX,
		p.gwUDPSendError,
		p.gwUDPReceiveError,
		p.udpResponseQueueDepth,
		p.udpResponseQueueFull,
		p.udpResponseDropped,
		p.http504Total,
		p.timeoutTotal,
		p.contextDeadlineTotal,
		p.httpReceived,
		p.httpRateLimited,
		p.httpRPS,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return p
}

// Handler trả exposition format Prometheus (OpenMetrics).
func (m *Metrics) Handler() http.Handler {
	if m == nil || m.prom == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
		})
	}
	return promhttp.HandlerFor(m.prom.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

func (m *Metrics) observeProm(dSeconds float64, status int, reason string, ok bool) {
	if m == nil || m.prom == nil {
		return
	}
	result := "fail"
	if ok {
		result = "ok"
	}
	code := strconv.Itoa(status)
	m.prom.requestsTotal.WithLabelValues(code, result).Inc()
	m.prom.requestDuration.Observe(dSeconds)
	if !ok {
		m.prom.requestsFail.WithLabelValues(code, reason).Inc()
	}
}
