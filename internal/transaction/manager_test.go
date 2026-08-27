package transaction

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/registry"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

func dataReq(tx uint64, msgID uint32) *pb.Envelope {
	return &pb.Envelope{
		Type:          pb.MessageType_MESSAGE_TYPE_DATA_REQUEST,
		TransactionId: tx,
		Body:          &pb.Envelope_DataRequest{DataRequest: &pb.DataRequest{MessageId: msgID, Payload: []byte("q")}},
	}
}

func dataResp(tx uint64, msgID uint32) *pb.Envelope {
	return &pb.Envelope{
		Type:          pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE,
		TransactionId: tx,
		TraceId:       "r-" + string(rune('a'+tx%26)),
		Body:          &pb.Envelope_DataResponse{DataResponse: &pb.DataResponse{MessageId: msgID, Status: 0}},
	}
}

func TestCreate(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	id, err := m.Create(100)
	if err != nil || id != 100 {
		t.Fatalf("Create: %d %v", id, err)
	}
}

func TestCreateAutoID(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	id, err := m.Create(0)
	if err != nil || id == 0 {
		t.Fatalf("auto id=%d err=%v", id, err)
	}
}

func TestDuplicateCreate(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	if _, err := m.Create(7); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(7); !errors.Is(err, ErrTransactionExists) {
		t.Fatalf("err = %v", err)
	}
}

func TestCompleteWait(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	if _, err := m.Create(100); err != nil {
		t.Fatal(err)
	}
	resp := dataResp(100, 1)
	if err := m.Complete(100, resp); err != nil {
		t.Fatal(err)
	}
	got, err := m.Wait(context.Background(), 100)
	if err != nil || got.GetTransactionId() != 100 {
		t.Fatalf("got %+v err=%v", got, err)
	}
}

func TestCompleteBeforeWait(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	if _, err := m.Create(100); err != nil {
		t.Fatal(err)
	}
	if err := m.Complete(100, dataResp(100, 1)); err != nil {
		t.Fatal(err)
	}
	got, err := m.Wait(context.Background(), 100)
	if err != nil || got.GetTransactionId() != 100 {
		t.Fatalf("got %+v err=%v", got, err)
	}
}

func TestWaitUnknown(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	_, err := m.Wait(context.Background(), 9)
	if !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("err = %v", err)
	}
}

func TestCompleteUnknown(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	if err := m.Complete(9, dataResp(9, 1)); !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("err = %v", err)
	}
}

func TestCancel(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	if _, err := m.Create(5); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { _, err := m.Wait(context.Background(), 5); errCh <- err }()
	time.Sleep(5 * time.Millisecond)
	if err := m.Cancel(5); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("wait err = %v", err)
	}
	if err := m.Complete(5, dataResp(5, 1)); !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("after cancel complete = %v", err)
	}
}

func TestTimeout(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	id, err := m.Create(0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_, err = m.Wait(ctx, id)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if err := m.Complete(id, dataResp(id, 1)); !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("after timeout still pending: %v", err)
	}
}

func TestContextCancel(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	id, _ := m.Create(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.Wait(ctx, id)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if err := m.Complete(id, dataResp(id, 1)); !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("still pending: %v", err)
	}
}

func TestDuplicateComplete(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	if _, err := m.Create(100); err != nil {
		t.Fatal(err)
	}
	first := dataResp(100, 1)
	first.TraceId = "first"
	if err := m.Complete(100, first); err != nil {
		t.Fatal(err)
	}
	second := dataResp(100, 1)
	second.TraceId = "second"
	if err := m.Complete(100, second); !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("second complete = %v", err)
	}
	got, err := m.Wait(context.Background(), 100)
	if err != nil || got.GetTraceId() != "first" {
		t.Fatalf("got %+v err=%v", got, err)
	}
}

func TestManagerClose(t *testing.T) {
	t.Parallel()
	m := NewManager()
	if _, err := m.Create(1); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { _, err := m.Wait(context.Background(), 1); errCh <- err }()
	time.Sleep(5 * time.Millisecond)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := <-errCh; !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("wait = %v", err)
	}
}

func TestCreateAfterClose(t *testing.T) {
	t.Parallel()
	m := NewManager()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(1); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("err = %v", err)
	}
	if _, err := m.Wait(context.Background(), 1); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("wait = %v", err)
	}
}

func TestOutOfOrderComplete(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	_, _ = m.Create(100)
	_, _ = m.Create(200)
	r200 := dataResp(200, 1)
	r200.TraceId = "200"
	r100 := dataResp(100, 1)
	r100.TraceId = "100"
	if err := m.Complete(200, r200); err != nil {
		t.Fatal(err)
	}
	if err := m.Complete(100, r100); err != nil {
		t.Fatal(err)
	}
	a, err := m.Wait(context.Background(), 100)
	if err != nil || a.GetTraceId() != "100" {
		t.Fatalf("100: %+v %v", a, err)
	}
	b, err := m.Wait(context.Background(), 200)
	if err != nil || b.GetTraceId() != "200" {
		t.Fatalf("200: %+v %v", b, err)
	}
}

type stubRouter struct {
	node *registry.Node
	err  error
	n    atomic.Int32
}

func (s *stubRouter) Route(context.Context, *pb.Envelope) (*registry.Node, error) {
	s.n.Add(1)
	return s.node, s.err
}

type stubTransport struct {
	err    error
	sends  atomic.Int32
	mu     sync.Mutex
	last   *pb.Envelope
	onSend func(*pb.Envelope)
}

func (s *stubTransport) Send(_ context.Context, msg *pb.Envelope, _ *net.UDPAddr) error {
	s.sends.Add(1)
	s.mu.Lock()
	s.last = msg
	cb := s.onSend
	s.mu.Unlock()
	if cb != nil {
		cb(msg)
	}
	return s.err
}
func (s *stubTransport) Receive(context.Context) (*pb.Envelope, *net.UDPAddr, error) {
	return nil, nil, errors.New("Receive must not be called")
}
func (s *stubTransport) Close() error        { return nil }
func (s *stubTransport) LocalAddr() net.Addr { return nil }

func (s *stubTransport) lastMsg() *pb.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func destNode() *registry.Node {
	return &registry.Node{ID: 1, Type: registry.TypeLogic, Address: "127.0.0.1", UDPPort: 9100}
}

func TestRequestSuccess(t *testing.T) {
	t.Parallel()
	m := NewManager()
	tr := &stubTransport{}
	svc, err := NewService(m, &stubRouter{node: destNode()}, tr, Config{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close() })

	errCh := make(chan error, 1)
	var got *pb.Envelope
	go func() {
		var e error
		got, e = svc.Request(context.Background(), dataReq(0, 1001))
		errCh <- e
	}()
	deadline := time.Now().Add(time.Second)
	for tr.lastMsg() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	sent := tr.lastMsg()
	if sent == nil {
		t.Fatal("Send not called")
	}
	if err := svc.OnResponse(dataResp(sent.GetTransactionId(), 1001)); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if got.GetType() != pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE {
		t.Fatalf("got %+v", got)
	}
}

func TestRequestRouteFailure(t *testing.T) {
	t.Parallel()
	m := NewManager()
	tr := &stubTransport{}
	rt := &stubRouter{err: errors.New("no route")}
	svc, _ := NewService(m, rt, tr, Config{Timeout: time.Second})
	t.Cleanup(func() { _ = svc.Close() })
	_, err := svc.Request(context.Background(), dataReq(11, 1001))
	if err == nil || rt.n.Load() != 1 || tr.sends.Load() != 0 {
		t.Fatalf("err=%v routeCalls=%d sends=%d", err, rt.n.Load(), tr.sends.Load())
	}
	if err := m.Complete(11, dataResp(11, 1001)); !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("tx not cleaned: %v", err)
	}
}

func TestRequestSendFailure(t *testing.T) {
	t.Parallel()
	m := NewManager()
	sentinel := errors.New("udp send failed")
	tr := &stubTransport{err: sentinel}
	svc, _ := NewService(m, &stubRouter{node: destNode()}, tr, Config{Timeout: time.Second})
	t.Cleanup(func() { _ = svc.Close() })
	_, err := svc.Request(context.Background(), dataReq(12, 1001))
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v", err)
	}
	// Send fail → 1 failover retry → 2 sends (cùng node nếu stub không exclude).
	if tr.sends.Load() != 2 {
		t.Fatalf("sends = %d want 2", tr.sends.Load())
	}
	if err := m.Complete(12, dataResp(12, 1001)); !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("tx not cleaned: %v", err)
	}
}

type stubExcludeRouter struct {
	nodes []*registry.Node
	n     atomic.Int32
}

func (s *stubExcludeRouter) Route(ctx context.Context, env *pb.Envelope) (*registry.Node, error) {
	return s.RouteExcluding(ctx, env)
}

func (s *stubExcludeRouter) RouteExcluding(_ context.Context, _ *pb.Envelope, exclude ...uint32) (*registry.Node, error) {
	s.n.Add(1)
	ex := map[uint32]struct{}{}
	for _, id := range exclude {
		ex[id] = struct{}{}
	}
	for _, n := range s.nodes {
		if _, skip := ex[n.ID]; skip {
			continue
		}
		return n, nil
	}
	return nil, errors.New("no eligible")
}

func TestRequestFailoverAfterSendFail(t *testing.T) {
	t.Parallel()
	m := NewManager()
	n1 := &registry.Node{ID: 2, Type: registry.TypeLogic, Address: "127.0.0.1", UDPPort: 9100}
	n2 := &registry.Node{ID: 3, Type: registry.TypeLogic, Address: "127.0.0.1", UDPPort: 9101}
	rt := &stubExcludeRouter{nodes: []*registry.Node{n1, n2}}
	tr := &stubTransport{}
	var sendN atomic.Int32
	tr.onSend = func(env *pb.Envelope) {
		if sendN.Add(1) == 1 {
			tr.err = errors.New("udp down")
		} else {
			tr.err = nil
			go func() {
				time.Sleep(5 * time.Millisecond)
				_ = m.Complete(env.GetTransactionId(), dataResp(env.GetTransactionId(), 1001))
			}()
		}
	}
	svc, err := NewService(m, rt, tr, Config{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close() })

	got, err := svc.Request(context.Background(), dataReq(0, 1001))
	if err != nil {
		t.Fatal(err)
	}
	if got.GetType() != pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE {
		t.Fatalf("got %+v", got)
	}
	if rt.n.Load() < 2 || tr.sends.Load() < 2 {
		t.Fatalf("routeCalls=%d sends=%d", rt.n.Load(), tr.sends.Load())
	}
	last := tr.lastMsg()
	if last == nil || last.GetDestinationNodeId() != 3 {
		t.Fatalf("last dest=%v want 3", last)
	}
}

func TestRequestTimeout(t *testing.T) {
	t.Parallel()
	m := NewManager()
	svc, _ := NewService(m, &stubRouter{node: destNode()}, &stubTransport{}, Config{Timeout: 20 * time.Millisecond})
	t.Cleanup(func() { _ = svc.Close() })
	_, err := svc.Request(context.Background(), dataReq(13, 1001))
	if !errors.Is(err, ErrTransactionTimeout) {
		t.Fatalf("err = %v", err)
	}
	if err := m.Complete(13, dataResp(13, 1001)); !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("tx not cleaned: %v", err)
	}
}


func TestRequestContextCancel(t *testing.T) {
	t.Parallel()
	m := NewManager()
	tr := &stubTransport{}
	svc, _ := NewService(m, &stubRouter{node: destNode()}, tr, Config{Timeout: time.Second})
	t.Cleanup(func() { _ = svc.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := svc.Request(ctx, dataReq(14, 1001))
		errCh <- err
	}()
	deadline := time.Now().Add(time.Second)
	for tr.lastMsg() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if tr.lastMsg() == nil {
		t.Fatal("Send not called")
	}
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if err := m.Complete(14, dataResp(14, 1001)); !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("late response resurrected tx: %v", err)
	}
}

func TestOnResponse(t *testing.T) {
	t.Parallel()
	m := NewManager()
	svc, _ := NewService(m, &stubRouter{node: destNode()}, &stubTransport{}, Config{})
	t.Cleanup(func() { _ = svc.Close() })
	if _, err := m.Create(30); err != nil {
		t.Fatal(err)
	}
	resp := dataResp(30, 1001)
	if err := svc.OnResponse(resp); err != nil {
		t.Fatal(err)
	}
	got, err := m.Wait(context.Background(), 30)
	if err != nil || got.GetTransactionId() != 30 {
		t.Fatalf("got %+v err=%v", got, err)
	}
}

func TestOnResponseWrongMessageType(t *testing.T) {
	t.Parallel()
	m := NewManager()
	svc, _ := NewService(m, &stubRouter{node: destNode()}, &stubTransport{}, Config{})
	t.Cleanup(func() { _ = svc.Close() })
	if _, err := m.Create(20); err != nil {
		t.Fatal(err)
	}
	hb := &pb.Envelope{Type: pb.MessageType_MESSAGE_TYPE_HEARTBEAT_RESPONSE, TransactionId: 20}
	if err := svc.OnResponse(hb); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("err = %v", err)
	}
	reg := &pb.Envelope{Type: pb.MessageType_MESSAGE_TYPE_REGISTER_RESPONSE, TransactionId: 20}
	if err := svc.OnResponse(reg); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("err = %v", err)
	}
}

func TestOnResponseUnknownTransaction(t *testing.T) {
	t.Parallel()
	m := NewManager()
	svc, _ := NewService(m, &stubRouter{node: destNode()}, &stubTransport{}, Config{})
	t.Cleanup(func() { _ = svc.Close() })
	if err := svc.OnResponse(dataResp(99, 1)); !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("err = %v", err)
	}
}

func TestServiceOutOfOrder(t *testing.T) {
	t.Parallel()
	m := NewManager()
	tr := &stubTransport{}
	svc, _ := NewService(m, &stubRouter{node: destNode()}, tr, Config{Timeout: time.Second})
	t.Cleanup(func() { _ = svc.Close() })

	type rec struct {
		env *pb.Envelope
		err error
	}
	ch100 := make(chan rec, 1)
	ch200 := make(chan rec, 1)
	go func() {
		e, err := svc.Request(context.Background(), dataReq(100, 1001))
		ch100 <- rec{e, err}
	}()
	go func() {
		e, err := svc.Request(context.Background(), dataReq(200, 1001))
		ch200 <- rec{e, err}
	}()

	deadline := time.Now().Add(time.Second)
	for m.has(100) && m.has(200) && time.Now().Before(deadline) {
		// both created once Request passed Create; might still be routing
		time.Sleep(time.Millisecond)
	}
	// Wait until both pending (Create done). Poll Complete until success.
	for time.Now().Before(deadline) {
		err200 := svc.OnResponse(func() *pb.Envelope { e := dataResp(200, 1001); e.TraceId = "200"; return e }())
		err100 := svc.OnResponse(func() *pb.Envelope { e := dataResp(100, 1001); e.TraceId = "100"; return e }())
		if err200 == nil && err100 == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}

	a := <-ch100
	b := <-ch200
	if a.err != nil || a.env.GetTraceId() != "100" {
		t.Fatalf("100: %+v %v", a.env, a.err)
	}
	if b.err != nil || b.env.GetTraceId() != "200" {
		t.Fatalf("200: %+v %v", b.env, b.err)
	}
}

func TestConcurrentRequests(t *testing.T) {
	t.Parallel()
	m := NewManager()
	tr := &stubTransport{onSend: func(msg *pb.Envelope) {}}
	svc, _ := NewService(m, &stubRouter{node: destNode()}, tr, Config{Timeout: 3 * time.Second})
	t.Cleanup(func() { _ = svc.Close() })

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	errs := make(chan error, n)
	for i := 1; i <= n; i++ {
		go func(id uint64) {
			defer wg.Done()
			got, err := svc.Request(context.Background(), dataReq(id, 1001))
			if err != nil {
				errs <- err
				return
			}
			if got.GetTransactionId() != id {
				errs <- errors.New("mismatched id")
			}
		}(uint64(i))
	}

	deadline := time.Now().Add(3 * time.Second)
	delivered := make(map[uint64]bool, n)
	for len(delivered) < n && time.Now().Before(deadline) {
		for id := uint64(1); id <= n; id++ {
			if delivered[id] {
				continue
			}
			resp := dataResp(id, 1001)
			resp.TraceId = "ok"
			if err := svc.OnResponse(resp); err == nil {
				delivered[id] = true
			}
		}
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentWaitRejected(t *testing.T) {
	t.Parallel()
	m := NewManager()
	t.Cleanup(func() { _ = m.Close() })
	if _, err := m.Create(1); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		close(started)
		_, err := m.Wait(context.Background(), 1)
		errCh <- err
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	_, err := m.Wait(context.Background(), 1)
	if !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("second wait: %v", err)
	}
	if err := m.Complete(1, dataResp(1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestCloseCleansMap(t *testing.T) {
	t.Parallel()
	m := NewManager()
	if _, err := m.Create(3); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if m.has(3) {
		t.Fatal("pending leftover after Close")
	}
}
