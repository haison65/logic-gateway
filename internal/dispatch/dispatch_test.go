package dispatch

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/heartbeat"
	"github.com/haison65/logic-gateway/internal/metrics"
	"github.com/haison65/logic-gateway/internal/registration"
	"github.com/haison65/logic-gateway/internal/registry"
	"github.com/haison65/logic-gateway/internal/transport/udp"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
	"go.uber.org/zap"
)

type recCompleter struct {
	ch chan *pb.Envelope
}

func (c *recCompleter) OnResponse(env *pb.Envelope) error {
	c.ch <- env
	return nil
}

func newUDP(t *testing.T) *udp.Conn {
	t.Helper()
	c, err := udp.New(udp.Config{ListenHost: "127.0.0.1", ListenPort: 0, ReadPollInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestDispatcherRegisterAndHeartbeat(t *testing.T) {
	t.Parallel()
	gw := newUDP(t)
	logic := newUDP(t)
	reg := registry.NewMemory()
	comp := &recCompleter{ch: make(chan *pb.Envelope, 1)}
	d, err := New(1, gw, registration.NewService(reg), heartbeat.NewService(reg, nil), comp, zap.NewNop(), nil, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = d.Run(ctx) }()

	req := &pb.Envelope{
		Version:       1,
		Type:          pb.MessageType_MESSAGE_TYPE_REGISTER_REQUEST,
		SourceNodeId:  42,
		TransactionId: 7,
		Body: &pb.Envelope_RegisterRequest{RegisterRequest: &pb.RegisterRequest{
			NodeId:     42,
			NodeType:   pb.NodeType_NODE_TYPE_LOGIC,
			InstanceId: "logic-1",
			Ip:         "127.0.0.1",
			Port:       uint32(logic.LocalAddr().(*net.UDPAddr).Port),
			Services:   []*pb.ServiceCapability{{ServiceId: 100, ServiceName: "call", MessageTypes: []uint32{1001}}},
		}},
	}
	if err := logic.Send(ctx, req, gw.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	got, _, err := logic.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !got.GetRegisterResponse().GetAccepted() {
		t.Fatalf("register = %+v", got)
	}

	hb := &pb.Envelope{
		Version:       1,
		Type:          pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST,
		SourceNodeId:  42,
		TransactionId: 8,
		Body:          &pb.Envelope_HeartbeatRequest{HeartbeatRequest: &pb.HeartbeatRequest{NodeId: 42, Sequence: 1}},
	}
	if err := logic.Send(ctx, hb, gw.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	got, _, err = logic.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetHeartbeatResponse() == nil || got.GetHeartbeatResponse().GetSequence() != 1 {
		t.Fatalf("heartbeat = %+v", got)
	}

	data := &pb.Envelope{
		Type:          pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE,
		TransactionId: 99,
		Body:          &pb.Envelope_DataResponse{DataResponse: &pb.DataResponse{MessageId: 1001}},
	}
	if err := logic.Send(ctx, data, gw.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	select {
	case env := <-comp.ch:
		if env.GetTransactionId() != 99 {
			t.Fatalf("tx = %d", env.GetTransactionId())
		}
	case <-ctx.Done():
		t.Fatal("OnResponse not called")
	}
}

func TestDispatcherErrorEnvelopeCompletes(t *testing.T) {
	t.Parallel()
	gw := newUDP(t)
	logic := newUDP(t)
	reg := registry.NewMemory()
	comp := &recCompleter{ch: make(chan *pb.Envelope, 1)}
	d, err := New(1, gw, registration.NewService(reg), heartbeat.NewService(reg, nil), comp, zap.NewNop(), nil, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = d.Run(ctx) }()

	errEnv := &pb.Envelope{
		Type:          pb.MessageType_MESSAGE_TYPE_ERROR,
		TransactionId: 77,
		Body: &pb.Envelope_Error{Error: &pb.ErrorMessage{
			Code:    pb.ErrorCode_ERROR_CODE_OVERLOAD,
			Message: "busy",
		}},
	}
	if err := logic.Send(ctx, errEnv, gw.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	select {
	case env := <-comp.ch:
		if env.GetType() != pb.MessageType_MESSAGE_TYPE_ERROR || env.GetTransactionId() != 77 {
			t.Fatalf("got %+v", env)
		}
	case <-ctx.Done():
		t.Fatal("OnResponse not called for ERROR")
	}
}

func TestConfigNormalizedDefaults(t *testing.T) {
	t.Parallel()
	got := (Config{}).normalized()
	if got.InboundQueueSize != defaultInboundQueueSize || got.InboundWorkers != defaultInboundWorkers {
		t.Fatalf("defaults = %+v", got)
	}
	got = (Config{InboundQueueSize: 100, InboundWorkers: 4}).normalized()
	if got.InboundQueueSize != 100 || got.InboundWorkers != 4 {
		t.Fatalf("custom = %+v", got)
	}
}

func TestInboundQueueDropsWhenFull(t *testing.T) {
	t.Parallel()
	gw := newUDP(t)
	logic := newUDP(t)
	reg := registry.NewMemory()
	block := make(chan struct{})
	comp := &blockingCompleter{block: block}
	met := metrics.New()
	d, err := New(1, gw, registration.NewService(reg), heartbeat.NewService(reg, nil), comp, zap.NewNop(), nil, met, Config{
		InboundQueueSize: 1,
		InboundWorkers:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = d.Run(ctx) }()

	addr := gw.LocalAddr().(*net.UDPAddr)
	// First response occupies the single worker (blocked in Complete).
	first := &pb.Envelope{
		Type:          pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE,
		TransactionId: 1,
		Body:          &pb.Envelope_DataResponse{DataResponse: &pb.DataResponse{MessageId: 1001}},
	}
	if err := logic.Send(ctx, first, addr); err != nil {
		t.Fatal(err)
	}
	// Fill the 1-slot queue + overflow a few packets while worker blocked.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		env := &pb.Envelope{
			Type:          pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE,
			TransactionId: uint64(time.Now().UnixNano()),
			Body:          &pb.Envelope_DataResponse{DataResponse: &pb.DataResponse{MessageId: 1001}},
		}
		_ = logic.Send(ctx, env, addr)
		snap := met.Snapshot()
		if snap.UDPResponseDroppedTotal > 0 && snap.UDPResponseQueueFullTotal > 0 {
			close(block)
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(block)
	t.Fatalf("expected queue drops, got %+v", met.Snapshot())
}

type blockingCompleter struct {
	block chan struct{}
}

func (c *blockingCompleter) OnResponse(*pb.Envelope) error {
	<-c.block
	return nil
}

type fakeOutbound struct {
	data *pb.DataResponse
	err  error
}

func (f fakeOutbound) Configured() bool { return true }

func (f fakeOutbound) Forward(context.Context, *pb.Envelope) (*pb.DataResponse, error) {
	return f.data, f.err
}

func TestDispatcherOutboundHTTPSuccess(t *testing.T) {
	t.Parallel()
	gw := newUDP(t)
	logic := newUDP(t)
	reg := registry.NewMemory()
	comp := &recCompleter{ch: make(chan *pb.Envelope, 1)}
	ob := fakeOutbound{data: &pb.DataResponse{MessageId: 1001, Status: 200, Payload: []byte("remote")}}
	d, err := New(1, gw, registration.NewService(reg), heartbeat.NewService(reg, nil), comp, zap.NewNop(), ob, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = d.Run(ctx) }()

	req := &pb.Envelope{
		Version:       1,
		Type:          pb.MessageType_MESSAGE_TYPE_DATA_REQUEST,
		SourceNodeId:  42,
		TransactionId: 77,
		TraceId:       "tr",
		Body:          &pb.Envelope_DataRequest{DataRequest: &pb.DataRequest{MessageId: 1001, Payload: []byte("in")}},
	}
	if err := logic.Send(ctx, req, gw.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	got, _, err := logic.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetType() != pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE {
		t.Fatalf("type %s", got.GetType())
	}
	if got.GetTransactionId() != 77 {
		t.Fatalf("tx %d", got.GetTransactionId())
	}
	if string(got.GetDataResponse().GetPayload()) != "remote" || got.GetDataResponse().GetStatus() != 200 {
		t.Fatalf("body %+v", got.GetDataResponse())
	}
}

func TestDispatcherOutboundHTTPStatusesAndErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		ob     Outbound
		want   pb.MessageType
		status uint32
		code   pb.ErrorCode
	}{
		{name: "http4xx", ob: fakeOutbound{data: &pb.DataResponse{MessageId: 1001, Status: 400, Payload: []byte("bad")}}, want: pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE, status: 400},
		{name: "http5xx", ob: fakeOutbound{data: &pb.DataResponse{MessageId: 1001, Status: 503, Payload: []byte("down")}}, want: pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE, status: 503},
		{name: "timeout", ob: fakeOutbound{err: context.DeadlineExceeded}, want: pb.MessageType_MESSAGE_TYPE_ERROR, code: pb.ErrorCode_ERROR_CODE_TIMEOUT},
		{name: "conn", ob: fakeOutbound{err: errors.New("connection refused")}, want: pb.MessageType_MESSAGE_TYPE_ERROR, code: pb.ErrorCode_ERROR_CODE_NODE_NOT_AVAILABLE},
		{name: "noremote", ob: nil, want: pb.MessageType_MESSAGE_TYPE_ERROR, code: pb.ErrorCode_ERROR_CODE_NO_ROUTING_TARGET},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gw := newUDP(t)
			logic := newUDP(t)
			reg := registry.NewMemory()
			comp := &recCompleter{ch: make(chan *pb.Envelope, 1)}
			d, err := New(1, gw, registration.NewService(reg), heartbeat.NewService(reg, nil), comp, zap.NewNop(), tc.ob, nil, Config{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go func() { _ = d.Run(ctx) }()
			req := &pb.Envelope{
				Type:          pb.MessageType_MESSAGE_TYPE_DATA_REQUEST,
				SourceNodeId:  42,
				TransactionId: 9,
				Body:          &pb.Envelope_DataRequest{DataRequest: &pb.DataRequest{MessageId: 1001, Payload: []byte("x")}},
			}
			if err := logic.Send(ctx, req, gw.LocalAddr().(*net.UDPAddr)); err != nil {
				t.Fatal(err)
			}
			got, _, err := logic.Receive(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got.GetType() != tc.want {
				t.Fatalf("type %s", got.GetType())
			}
			if tc.want == pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE && got.GetDataResponse().GetStatus() != tc.status {
				t.Fatalf("status %d", got.GetDataResponse().GetStatus())
			}
			if tc.want == pb.MessageType_MESSAGE_TYPE_ERROR && got.GetError().GetCode() != tc.code {
				t.Fatalf("code %s", got.GetError().GetCode())
			}
			if got.GetTransactionId() != 9 {
				t.Fatalf("tx %d", got.GetTransactionId())
			}
		})
	}
}
