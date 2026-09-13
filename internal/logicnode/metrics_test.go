package logicnode

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNodeMetricsAddRequest(t *testing.T) {
	t.Parallel()
	m := newNodeMetrics(2, "logic-1")
	m.AddRequest()
	m.AddRequest()
	rr := httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	text := string(body)
	if !strings.Contains(text, "logic_requests_total") {
		t.Fatalf("missing logic_requests_total:\n%s", text)
	}
	if !strings.Contains(text, "logic_udp_data_request_rx_total") {
		t.Fatalf("missing udp_data_request_rx:\n%s", text)
	}
	if !strings.Contains(text, `node="logic-1"`) || !strings.Contains(text, `node_id="2"`) {
		t.Fatalf("missing labels:\n%s", text)
	}
	if !strings.Contains(text, "logic_requests_total{") || !strings.Contains(text, "} 2") {
		// counter line ends with " 2" or " 2\n"
		found := false
		for _, line := range strings.Split(text, "\n") {
			if strings.HasPrefix(line, "logic_requests_total{") && strings.HasSuffix(strings.TrimSpace(line), " 2") {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected counter value 2:\n%s", text)
		}
	}
}

func TestNodeMetricsDesignSection17(t *testing.T) {
	t.Parallel()
	m := newNodeMetrics(3, "logic-metrics")
	m.SetRegisterStatus(true)
	m.SetHeartbeatRTT(12 * time.Millisecond)
	m.SetQueueSize(7)
	rr := httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rr.Body)
	text := string(body)
	for _, want := range []string{
		"logic_register_status",
		"logic_heartbeat_rtt_seconds",
		"logic_queue_size",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %s:\n%s", want, text)
		}
	}
	if !strings.Contains(text, "logic_register_status{") || !strings.Contains(text, "} 1") {
		found := false
		for _, line := range strings.Split(text, "\n") {
			if strings.HasPrefix(line, "logic_register_status{") && strings.HasSuffix(strings.TrimSpace(line), " 1") {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("register_status != 1:\n%s", text)
		}
	}
}
