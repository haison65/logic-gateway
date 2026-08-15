package registration

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

func asUDPAddr(t *testing.T, a net.Addr) *net.UDPAddr {
	t.Helper()
	ua, ok := a.(*net.UDPAddr)
	if !ok {
		t.Fatalf("addr type = %T", a)
	}
	return ua
}

func TestRegisterOverUDP(t *testing.T) {
	t.Parallel()

	gateway := newUDP(t)
	logic := newUDP(t)
	reg := registry.NewMemory()
	svc := NewService(reg)

	req := validRequest()
	reqEnv := &pb.Envelope{
		Version:           1,
		Type:              pb.MessageType_MESSAGE_TYPE_REGISTER_REQUEST,
		SourceNodeId:      req.GetNodeId(),
		DestinationNodeId: 0,
		TransactionId:     77,
		TimestampMs:       1_725_000_000_000,
		TraceId:           "reg-udp",
		Body:              &pb.Envelope_RegisterRequest{RegisterRequest: req},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type recvResult struct {
		msg  *pb.Envelope
		from *net.UDPAddr
		err  error
	}
	gotCh := make(chan recvResult, 1)
	go func() {
		msg, from, err := gateway.Receive(ctx)
		gotCh <- recvResult{msg: msg, from: from, err: err}
	}()

	if err := logic.Send(ctx, reqEnv, asUDPAddr(t, gateway.LocalAddr())); err != nil {
		t.Fatalf("logic Send: %v", err)
	}

	got := <-gotCh
	if got.err != nil {
		t.Fatalf("gateway Receive: %v", got.err)
	}
	if got.from == nil || got.from.Port == 0 {
		t.Fatalf("missing sender address: %v", got.from)
	}
	if got.msg.GetType() != pb.MessageType_MESSAGE_TYPE_REGISTER_REQUEST {
		t.Fatalf("type = %v", got.msg.GetType())
	}
	if got.msg.GetTransactionId() != 77 || got.msg.GetTraceId() != "reg-udp" {
		t.Fatalf("envelope metadata = %+v", got.msg)
	}
	in := got.msg.GetRegisterRequest()
	if in == nil {
		t.Fatal("missing RegisterRequest body")
	}

	resp, err := svc.Register(in)
	if err != nil {
		t.Fatalf("service Register: %v", err)
	}
	if !resp.GetAccepted() || resp.GetAssignedNodeId() != 42 {
		t.Fatalf("response = %+v", resp)
	}

	stored, ok := reg.Get(42)
	if !ok {
		t.Fatal("registry missing node 42")
	}
	if stored.InstanceID != "logic-1" || stored.State != registry.NodeStateRegistered {
		t.Fatalf("stored = %+v", stored)
	}

	respEnv := &pb.Envelope{
		Version:           1,
		Type:              pb.MessageType_MESSAGE_TYPE_REGISTER_RESPONSE,
		SourceNodeId:      0,
		DestinationNodeId: req.GetNodeId(),
		TransactionId:     got.msg.GetTransactionId(),
		TraceId:           got.msg.GetTraceId(),
		Body:              &pb.Envelope_RegisterResponse{RegisterResponse: resp},
	}
	if err := gateway.Send(ctx, respEnv, got.from); err != nil {
		t.Fatalf("gateway Send: %v", err)
	}

	out, fromGW, err := logic.Receive(ctx)
	if err != nil {
		t.Fatalf("logic Receive: %v", err)
	}
	if fromGW == nil {
		t.Fatal("expected gateway address")
	}
	if out.GetType() != pb.MessageType_MESSAGE_TYPE_REGISTER_RESPONSE {
		t.Fatalf("response type = %v", out.GetType())
	}
	decoded := out.GetRegisterResponse()
	if decoded == nil || !decoded.GetAccepted() || decoded.GetAssignedNodeId() != 42 {
		t.Fatalf("decoded response = %+v", decoded)
	}
}

func TestRegisterOverUDPRejected(t *testing.T) {
	t.Parallel()

	gateway := newUDP(t)
	logic := newUDP(t)
	svc := NewService(registry.NewMemory())

	bad := &pb.Envelope{
		Version: 1,
		Type:    pb.MessageType_MESSAGE_TYPE_REGISTER_REQUEST,
		TraceId: "bad-reg",
		Body: &pb.Envelope_RegisterRequest{
			RegisterRequest: &pb.RegisterRequest{
				NodeId:     0,
				NodeType:   pb.NodeType_NODE_TYPE_LOGIC,
				InstanceId: "x",
				Ip:         "127.0.0.1",
				Port:       1,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gotCh := make(chan *pb.Envelope, 1)
	errCh := make(chan error, 1)
	go func() {
		msg, from, err := gateway.Receive(ctx)
		if err != nil {
			errCh <- err
			return
		}
		resp, _ := svc.Register(msg.GetRegisterRequest())
		respEnv := &pb.Envelope{
			Version: 1,
			Type:    pb.MessageType_MESSAGE_TYPE_REGISTER_RESPONSE,
			TraceId: msg.GetTraceId(),
			Body:    &pb.Envelope_RegisterResponse{RegisterResponse: resp},
		}
		if err := gateway.Send(ctx, respEnv, from); err != nil {
			errCh <- err
			return
		}
		gotCh <- msg
	}()

	if err := logic.Send(ctx, bad, asUDPAddr(t, gateway.LocalAddr())); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("gateway: %v", err)
	case <-gotCh:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	out, _, err := logic.Receive(ctx)
	if err != nil {
		t.Fatalf("logic Receive: %v", err)
	}
	resp := out.GetRegisterResponse()
	if resp == nil || resp.GetAccepted() {
		t.Fatalf("expected rejected response, got %+v", resp)
	}
}
