package logicnode

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/config"
	"github.com/haison65/logic-gateway/internal/transport/udp"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
	"go.uber.org/zap"
)

func newUDP(t *testing.T) *udp.Conn {
	t.Helper()
	c, err := udp.New(udp.Config{ListenHost: "127.0.0.1", ListenPort: 0, ReadPollInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestRegisterHeartbeatEcho(t *testing.T) {
	t.Parallel()
	gw := newUDP(t)
	gwAddr := gw.LocalAddr().(*net.UDPAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := config.Logic{
		Node:      config.Node{NodeID: 2, InstanceID: "logic-test", IP: "127.0.0.1"},
		UDP:       config.UDP{Listen: "127.0.0.1", Port: 0},
		Gateway:   config.Gateway{Host: gwAddr.IP.String(), Port: gwAddr.Port},
		Heartbeat: config.Heartbeat{Interval: config.Duration(30 * time.Millisecond)},
		Services:  []config.Service{{ServiceID: 100, ServiceName: "call", MessageTypes: []uint32{1001}}},
	}
	log := zap.NewNop()
	go func() { _ = Run(ctx, cfg, log) }()

	reg, from, err := gw.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reg.GetType() != pb.MessageType_MESSAGE_TYPE_REGISTER_REQUEST || reg.GetRegisterRequest().GetNodeId() != 2 {
		t.Fatalf("register = %+v", reg)
	}
	ack := &pb.Envelope{
		Version:           1,
		Type:              pb.MessageType_MESSAGE_TYPE_REGISTER_RESPONSE,
		SourceNodeId:      1,
		DestinationNodeId: 2,
		Body: &pb.Envelope_RegisterResponse{RegisterResponse: &pb.RegisterResponse{
			Accepted: true, AssignedNodeId: 2, Message: "registered",
		}},
	}
	if err := gw.Send(ctx, ack, from); err != nil {
		t.Fatal(err)
	}

	hb, _, err := gw.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if hb.GetType() != pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST {
		t.Fatalf("expected heartbeat, got %s", hb.GetType())
	}

	req := &pb.Envelope{
		Version:           1,
		Type:              pb.MessageType_MESSAGE_TYPE_DATA_REQUEST,
		SourceNodeId:      1,
		DestinationNodeId: 2,
		TransactionId:     77,
		TraceId:           "echo",
		Body:              &pb.Envelope_DataRequest{DataRequest: &pb.DataRequest{MessageId: 1001, Payload: []byte("ping")}},
	}
	if err := gw.Send(ctx, req, from); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		msg, _, err := gw.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if msg.GetType() == pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST {
			continue
		}
		if msg.GetType() != pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE {
			t.Fatalf("got %s", msg.GetType())
		}
		if msg.GetTransactionId() != 77 || string(msg.GetDataResponse().GetPayload()) != "ping" {
			t.Fatalf("response = %+v", msg)
		}
		if msg.GetDataResponse().GetStatus() != 200 {
			t.Fatalf("status = %d", msg.GetDataResponse().GetStatus())
		}
		return
	}
	t.Fatal("no DATA_RESPONSE")
}

func TestBusinessMessageIDsAndUnknown(t *testing.T) {
	t.Parallel()
	if handleBusiness(2, &pb.Envelope{TransactionId: 1, Body: &pb.Envelope_DataRequest{DataRequest: &pb.DataRequest{MessageId: 1002, Payload: []byte("u")}}}, 1002).GetType() != pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE {
		t.Fatal("1002")
	}
	if handleBusiness(2, &pb.Envelope{TransactionId: 1, Body: &pb.Envelope_DataRequest{DataRequest: &pb.DataRequest{MessageId: 1003, Payload: []byte("d")}}}, 1003).GetType() != pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE {
		t.Fatal("1003")
	}
	errEnv := handleBusiness(2, &pb.Envelope{TransactionId: 8, Body: &pb.Envelope_DataRequest{DataRequest: &pb.DataRequest{MessageId: 9}}}, 9)
	if errEnv.GetType() != pb.MessageType_MESSAGE_TYPE_ERROR || errEnv.GetError().GetCode() != pb.ErrorCode_ERROR_CODE_INVALID_MESSAGE {
		t.Fatalf("%+v", errEnv)
	}
	if errEnv.GetTransactionId() != 8 {
		t.Fatal(errEnv.GetTransactionId())
	}
}
