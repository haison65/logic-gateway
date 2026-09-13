package metrics

import (
	"testing"
	"time"
)

func TestObserveMinMaxAvg(t *testing.T) {
	t.Parallel()
	m := New()
	m.Observe(10*time.Millisecond, 200, ReasonOK)
	m.Observe(30*time.Millisecond, 200, ReasonOK)
	m.Observe(20*time.Millisecond, 503, ReasonNoRoutingTarget)
	s := m.Snapshot()
	if s.RequestsTotal != 3 || s.RequestsOK != 2 || s.RequestsFail != 1 {
		t.Fatalf("counts = %+v", s)
	}
	if s.Latency.MinMS != 10 || s.Latency.MaxMS != 30 || s.Latency.AvgMS != 20 {
		t.Fatalf("latency = %+v", s.Latency)
	}
	if s.ByStatus["200"] != 2 || s.ByStatus["503"] != 1 {
		t.Fatalf("by_status = %+v", s.ByStatus)
	}
	if s.FailByCode["503"] != 1 || s.FailByCode["504"] != 0 {
		t.Fatalf("fail_by_code = %+v", s.FailByCode)
	}
	if s.FailByReason[ReasonNoRoutingTarget] != 1 || s.FailByReason[ReasonTimeout] != 0 {
		t.Fatalf("fail_by_reason = %+v", s.FailByReason)
	}
	m.AddUDPRx(2)
	m.AddUDPTx(3)
	m.AddRouteFailed(1)
	m.AddTransactionTimeout(1)
	m.AddLogicRegistered(4)
	m.AddHeartbeatSuccess(5)
	m.AddHeartbeatTimeout(6)
	s = m.Snapshot()
	if s.UDPRXTotal != 2 || s.UDPTXTotal != 3 || s.RouteFailedTotal != 1 || s.TransactionTimeoutTotal != 1 {
		t.Fatalf("spec counters %+v", s)
	}
	if s.LogicRegisteredTotal != 4 || s.HeartbeatSuccessTotal != 5 || s.HeartbeatTimeoutTotal != 6 {
		t.Fatalf("hb counters %+v", s)
	}
}

func TestInFlight(t *testing.T) {
	t.Parallel()
	m := New()
	m.AddInFlight(1)
	m.AddInFlight(1)
	if m.Snapshot().InFlight != 2 {
		t.Fatal(m.Snapshot().InFlight)
	}
	m.AddInFlight(-2)
	if m.Snapshot().InFlight != 0 {
		t.Fatal(m.Snapshot().InFlight)
	}
}

func TestAddLogicRouted(t *testing.T) {
	t.Parallel()
	m := New()
	m.AddLogicRouted(2, "logic-1")
	m.AddLogicRouted(2, "logic-1")
	m.AddLogicRouted(3, "logic-2")
	s := m.Snapshot()
	if s.LogicRoutedByNode["logic-1"] != 2 || s.LogicRoutedByNode["logic-2"] != 1 {
		t.Fatalf("logic_routed_by_node = %+v", s.LogicRoutedByNode)
	}
	// empty name falls back to node_id
	m.AddLogicRouted(9, "")
	if m.Snapshot().LogicRoutedByNode["9"] != 1 {
		t.Fatalf("fallback label: %+v", m.Snapshot().LogicRoutedByNode)
	}
}

func TestManagerAndUDPDiagCounters(t *testing.T) {
	t.Parallel()
	m := New()
	m.AddManagerCreated(2)
	m.AddManagerCompleted(1)
	m.AddManagerTimeout(1)
	m.AddManagerCompleteNotFound(3)
	m.SetManagerPending(7)
	m.AddGWUDPDataRequestTX(4)
	m.AddGWUDPDataResponseRX(5)
	m.AddGWUDPSendError(1)
	m.AddGWUDPReceiveError(2)
	m.AddHTTP504(6)
	m.AddTimeout(6)
	m.AddContextDeadline(1)
	m.ObserveManagerWait(10 * time.Millisecond)
	m.ObserveTransactionTotal(20 * time.Millisecond)
	s := m.Snapshot()
	if s.ManagerCreatedTotal != 2 || s.ManagerCompletedTotal != 1 || s.ManagerTimeoutTotal != 1 {
		t.Fatalf("manager counters %+v", s)
	}
	if s.ManagerCompleteNotFoundTotal != 3 || s.ManagerPending != 7 {
		t.Fatalf("manager pending/miss %+v", s)
	}
	if s.GWUDPDataRequestTXTotal != 4 || s.GWUDPDataResponseRXTotal != 5 {
		t.Fatalf("udp data %+v", s)
	}
	if s.HTTP504Total != 6 || s.TimeoutTotal != 6 || s.ContextDeadlineTotal != 1 {
		t.Fatalf("504 counters %+v", s)
	}
}
