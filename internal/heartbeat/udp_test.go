package heartbeat

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/registry"
	"github.com/haison65/logic-gateway/internal/transport/udp"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

func newUDP(t *testing.T) *udp.Conn {
	t.Helper()
	c, err := udp.New(udp.Config{
		ListenHost:       "127.0.0.1",
		ListenPort:       0,
		ReadPollInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("udp.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func asUDP(t *testing.T, a net.Addr) *net.UDPAddr {
	t.Helper()
	ua, ok := a.(*net.UDPAddr)
	if !ok {
		t.Fatalf("addr type = %T", a)
	}
	return ua
}

func TestHeartbeatOverUDP(t *testing.T) {
	t.Parallel()

	gateway := newUDP(t)
	logic := newUDP(t)
	reg := registry.NewMemory()
	registerLogic(t, reg, 42)
	svc := NewService(reg, newFakeClock(time.Unix(1_700_000_123, 0)))

	reqEnv := &pb.Envelope{
		Version:       1,
		Type:          pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST,
		SourceNodeId:  42,
		TransactionId: 8,
		TraceId:       "hb-udp",
		Body: &pb.Envelope_HeartbeatRequest{
			HeartbeatRequest: &pb.HeartbeatRequest{
				NodeId:      42,
				Sequence:    11,
				TimestampMs: 1_700_000_000,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type result struct {
		msg  *pb.Envelope
		from *net.UDPAddr
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		msg, from, err := gateway.Receive(ctx)
		ch <- result{msg, from, err}
	}()

	if err := logic.Send(ctx, reqEnv, asUDP(t, gateway.LocalAddr())); err != nil {
		t.Fatalf("Send: %v", err)
	}
	got := <-ch
	if got.err != nil {
		t.Fatalf("Receive: %v", got.err)
	}
	hb := got.msg.GetHeartbeatRequest()
	if hb == nil || hb.GetNodeId() != 42 {
		t.Fatalf("request = %+v", got.msg)
	}

	resp, err := svc.Handle(hb)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	n, ok := reg.Get(42)
	if !ok || n.State != registry.NodeStateActive {
		t.Fatalf("node = %+v ok=%v", n, ok)
	}

	respEnv := &pb.Envelope{
		Version:           1,
		Type:              pb.MessageType_MESSAGE_TYPE_HEARTBEAT_RESPONSE,
		DestinationNodeId: 42,
		TransactionId:     got.msg.GetTransactionId(),
		TraceId:           got.msg.GetTraceId(),
		Body:              &pb.Envelope_HeartbeatResponse{HeartbeatResponse: resp},
	}
	if err := gateway.Send(ctx, respEnv, got.from); err != nil {
		t.Fatalf("Send response: %v", err)
	}

	out, fromGW, err := logic.Receive(ctx)
	if err != nil {
		t.Fatalf("logic Receive: %v", err)
	}
	if fromGW == nil {
		t.Fatal("expected gateway address")
	}
	decoded := out.GetHeartbeatResponse()
	if decoded == nil || decoded.GetSequence() != 11 {
		t.Fatalf("response = %+v", decoded)
	}
}
