package heartbeat

import (
	"context"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/registry"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
	"go.uber.org/zap"
)

func TestSenderSendsHeartbeatToLogic(t *testing.T) {
	t.Parallel()

	gw := newUDP(t)
	logic := newUDP(t)
	reg := registry.NewMemory()
	logicAddr := asUDP(t, logic.LocalAddr())
	if err := reg.Register(&registry.Node{
		ID:      2,
		Type:    registry.TypeLogic,
		Address: logicAddr.IP.String(),
		UDPPort: uint32(logicAddr.Port),
	}); err != nil {
		t.Fatal(err)
	}
	// HTTP2GW trong registry không phải đích gửi.
	if err := reg.Register(&registry.Node{
		ID: 1, Type: registry.TypeHTTP2GW, Address: "127.0.0.1", UDPPort: 9000,
	}); err != nil {
		t.Fatal(err)
	}

	sender, err := NewSender(1, reg, gw, 50*time.Millisecond, nil, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type result struct {
		msg *pb.Envelope
		err error
	}
	ch := make(chan result, 1)
	go func() {
		msg, _, err := logic.Receive(ctx)
		ch <- result{msg, err}
	}()

	go func() { _ = sender.Run(ctx) }()

	got := <-ch
	if got.err != nil {
		t.Fatalf("Receive: %v", got.err)
	}
	if got.msg.GetType() != pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST {
		t.Fatalf("type = %s", got.msg.GetType())
	}
	hb := got.msg.GetHeartbeatRequest()
	if hb == nil || hb.GetNodeId() != 1 || hb.GetSequence() == 0 {
		t.Fatalf("heartbeat = %+v", hb)
	}
}

func TestSenderSkipsDeadLogic(t *testing.T) {
	t.Parallel()

	gw := newUDP(t)
	logic := newUDP(t)
	reg := registry.NewMemory()
	logicAddr := asUDP(t, logic.LocalAddr())
	if err := reg.Register(&registry.Node{
		ID: 2, Type: registry.TypeLogic,
		Address: logicAddr.IP.String(), UDPPort: uint32(logicAddr.Port),
	}); err != nil {
		t.Fatal(err)
	}
	// Đưa Logic sang DEAD qua timeout (Register đặt LastHeartbeatAt = now).
	far := time.Now().Add(time.Hour)
	_ = reg.ApplyTimeouts(far, func(state registry.NodeState, last time.Time, now time.Time) (registry.NodeState, bool) {
		return NextOnTimeout(state, now.Sub(last), time.Millisecond, 2*time.Millisecond)
	})
	n, ok := reg.Get(2)
	if !ok || n.State != registry.NodeStateDead {
		t.Fatalf("want DEAD, got %+v ok=%v", n, ok)
	}

	sender, err := NewSender(1, reg, gw, 30*time.Millisecond, nil, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_, _, _ = logic.Receive(ctx)
		close(done)
	}()
	go func() { _ = sender.Run(ctx) }()

	select {
	case <-done:
		t.Fatal("không kỳ vọng nhận HEARTBEAT khi Logic DEAD")
	case <-ctx.Done():
	}
}

func TestNewSenderRejectsBadArgs(t *testing.T) {
	t.Parallel()
	gw := newUDP(t)
	reg := registry.NewMemory()
	if _, err := NewSender(0, reg, gw, time.Second, nil, nil); err == nil {
		t.Fatal("expected node_id error")
	}
	if _, err := NewSender(1, nil, gw, time.Second, nil, nil); err == nil {
		t.Fatal("expected registry error")
	}
	if _, err := NewSender(1, reg, nil, time.Second, nil, nil); err == nil {
		t.Fatal("expected transport error")
	}
	if _, err := NewSender(1, reg, gw, 0, nil, nil); err == nil {
		t.Fatal("expected interval error")
	}
}

func TestLogicUDPAddr(t *testing.T) {
	t.Parallel()
	_, err := logicUDPAddr(&registry.Node{Address: "bad", UDPPort: 9100})
	if err == nil {
		t.Fatal("expected bad address")
	}
	addr, err := logicUDPAddr(&registry.Node{Address: "127.0.0.1", UDPPort: 9100})
	if err != nil || addr.Port != 9100 {
		t.Fatalf("addr=%v err=%v", addr, err)
	}
}
