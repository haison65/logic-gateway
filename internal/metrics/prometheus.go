package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "http2gw"

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
			Buckets:   []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
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
			Help:      "Transactions that timed out waiting for DATA_RESPONSE.",
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
