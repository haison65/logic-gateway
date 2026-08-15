package udp

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

func newLoopback(t *testing.T) *Conn {
	t.Helper()
	c, err := New(Config{
		ListenHost:       "127.0.0.1",
		ListenPort:       0,
		ReadPollInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func udpAddr(t *testing.T, a net.Addr) *net.UDPAddr {
	t.Helper()
	ua, ok := a.(*net.UDPAddr)
	if !ok {
		t.Fatalf("local addr type = %T, want *net.UDPAddr", a)
	}
	return ua
}

func TestSendReceiveDataRequest(t *testing.T) {
	t.Parallel()

	receiver := newLoopback(t)
	sender := newLoopback(t)

	original := &pb.Envelope{
		Version:           1,
		Type:              pb.MessageType_MESSAGE_TYPE_DATA_REQUEST,
		SourceNodeId:      1,
		DestinationNodeId: 2,
		TransactionId:     12345,
		TimestampMs:       1_725_000_000_000,
		TraceId:           "test-trace",
		Body: &pb.Envelope_DataRequest{
			DataRequest: &pb.DataRequest{
				MessageId: 1001,
				SessionId: "sess-1",
				Payload:   []byte("hello-udp"),
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	var got *pb.Envelope
	var from *net.UDPAddr
	go func() {
		var err error
		got, from, err = receiver.Receive(ctx)
		errCh <- err
	}()

	if err := sender.Send(ctx, original, udpAddr(t, receiver.LocalAddr())); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if from == nil {
		t.Fatal("expected sender address")
	}
	if from.IP == nil || from.Port == 0 {
		t.Fatalf("unexpected sender address: %v", from)
	}
	if !proto.Equal(original, got) {
		t.Fatalf("messages differ\nwant = %v\ngot  = %v", original, got)
	}
}

func TestSendReceiveMultipleMessageTypes(t *testing.T) {
	t.Parallel()

	receiver := newLoopback(t)
	sender := newLoopback(t)
	dst := udpAddr(t, receiver.LocalAddr())

	messages := []*pb.Envelope{
		{
			Version:       1,
			Type:          pb.MessageType_MESSAGE_TYPE_REGISTER_REQUEST,
			TransactionId: 1,
			TraceId:       "reg",
			Body: &pb.Envelope_RegisterRequest{
				RegisterRequest: &pb.RegisterRequest{
					NodeId:     42,
					NodeType:   pb.NodeType_NODE_TYPE_LOGIC,
					InstanceId: "logic-1",
					Ip:         "127.0.0.1",
					Port:       9100,
					Services: []*pb.ServiceCapability{{
						ServiceId:    100,
						ServiceName:  "call",
						MessageTypes: []uint32{1001},
					}},
				},
			},
		},
		{
			Version:       1,
			Type:          pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST,
			TransactionId: 2,
			TraceId:       "hb",
			Body: &pb.Envelope_HeartbeatRequest{
				HeartbeatRequest: &pb.HeartbeatRequest{
					NodeId:            42,
					Sequence:          7,
					TimestampMs:       100,
					Load:              10,
					ActiveTransaction: 1,
				},
			},
		},
		{
			Version:       1,
			Type:          pb.MessageType_MESSAGE_TYPE_DATA_REQUEST,
			TransactionId: 3,
			TraceId:       "data",
			Body: &pb.Envelope_DataRequest{
				DataRequest: &pb.DataRequest{
					MessageId: 2001,
					SessionId: "s",
					Payload:   []byte{9, 8, 7},
				},
			},
		},
		{
			Version:       1,
			Type:          pb.MessageType_MESSAGE_TYPE_ERROR,
			TransactionId: 4,
			TraceId:       "err",
			Body: &pb.Envelope_Error{
				Error: &pb.ErrorMessage{
					Code:    pb.ErrorCode_ERROR_CODE_INVALID_MESSAGE,
					Message: "bad",
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i, original := range messages {
		if err := sender.Send(ctx, original, dst); err != nil {
			t.Fatalf("Send[%d]: %v", i, err)
		}
		got, _, err := receiver.Receive(ctx)
		if err != nil {
			t.Fatalf("Receive[%d]: %v", i, err)
		}
		if !proto.Equal(original, got) {
			t.Fatalf("message[%d] differs\nwant = %v\ngot  = %v", i, original, got)
		}
	}
}

func TestReceiveInvalidProtobuf(t *testing.T) {
	t.Parallel()

	receiver := newLoopback(t)
	raw, err := net.DialUDP("udp", nil, udpAddr(t, receiver.LocalAddr()))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })

	// Varint bị cắt — dữ liệu protobuf không hợp lệ.
	if _, err := raw.Write([]byte{0x80}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	msg, addr, err := receiver.Receive(ctx)
	if msg != nil {
		t.Fatalf("expected nil message, got %v", msg)
	}
	if addr == nil {
		t.Fatal("expected sender address even for malformed packet")
	}
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("err = %v, want ErrInvalidMessage", err)
	}

	// Transport vẫn dùng được sau một gói lỗi định dạng.
	sender := newLoopback(t)
	okMsg := &pb.Envelope{
		Version:       1,
		Type:          pb.MessageType_MESSAGE_TYPE_DATA_REQUEST,
		TransactionId: 99,
		TraceId:       "after-bad",
		Body: &pb.Envelope_DataRequest{
			DataRequest: &pb.DataRequest{MessageId: 1, Payload: []byte("ok")},
		},
	}
	if err := sender.Send(ctx, okMsg, udpAddr(t, receiver.LocalAddr())); err != nil {
		t.Fatalf("Send after bad packet: %v", err)
	}
	got, _, err := receiver.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive after bad packet: %v", err)
	}
	if !proto.Equal(okMsg, got) {
		t.Fatalf("messages differ after recovery")
	}
}

func TestReceiveContextCancel(t *testing.T) {
	t.Parallel()

	receiver := newLoopback(t)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, _, err := receiver.Receive(ctx)
		errCh <- err
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Receive did not unblock after context cancel")
	}
}

func TestCloseUnblocksReceiveAndRejectsLaterOps(t *testing.T) {
	t.Parallel()

	receiver := newLoopback(t)
	ctx := context.Background()

	errCh := make(chan error, 1)
	go func() {
		_, _, err := receiver.Receive(ctx)
		errCh <- err
	}()

	time.Sleep(30 * time.Millisecond)
	if err := receiver.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := receiver.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrTransportClosed) {
			t.Fatalf("err = %v, want ErrTransportClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Receive did not unblock after Close")
	}

	sender := newLoopback(t)
	msg := &pb.Envelope{Version: 1, TraceId: "closed"}
	if err := receiver.Send(ctx, msg, udpAddr(t, sender.LocalAddr())); !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("Send after close: %v", err)
	}
	if _, _, err := receiver.Receive(ctx); !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("Receive after close: %v", err)
	}
}

func TestConcurrentSend(t *testing.T) {
	t.Parallel()

	receiver := newLoopback(t)
	sender := newLoopback(t)
	dst := udpAddr(t, receiver.LocalAddr())

	const n = 32
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			msg := &pb.Envelope{
				Version:       1,
				Type:          pb.MessageType_MESSAGE_TYPE_DATA_REQUEST,
				TransactionId: id,
				TraceId:       "concurrent",
				Body: &pb.Envelope_DataRequest{
					DataRequest: &pb.DataRequest{
						MessageId: uint32(id),
						Payload:   []byte("x"),
					},
				},
			}
			errCh <- sender.Send(ctx, msg, dst)
		}(uint64(i + 1))
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	seen := make(map[uint64]bool, n)
	for len(seen) < n {
		msg, _, err := receiver.Receive(ctx)
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		seen[msg.GetTransactionId()] = true
	}
}

func TestSendPacketTooLarge(t *testing.T) {
	t.Parallel()

	sender, err := New(Config{
		ListenHost:    "127.0.0.1",
		ListenPort:    0,
		MaxPacketSize: 8,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = sender.Close() })

	receiver := newLoopback(t)
	msg := &pb.Envelope{
		Version:       1,
		Type:          pb.MessageType_MESSAGE_TYPE_DATA_REQUEST,
		TransactionId: 1,
		TraceId:       "too-large-trace-id",
		Body: &pb.Envelope_DataRequest{
			DataRequest: &pb.DataRequest{Payload: []byte("0123456789abcdef")},
		},
	}

	err = sender.Send(context.Background(), msg, udpAddr(t, receiver.LocalAddr()))
	if !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("err = %v, want ErrPacketTooLarge", err)
	}
}

func TestCodecRoundTrip(t *testing.T) {
	t.Parallel()

	original := &pb.Envelope{
		Version:       1,
		Type:          pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST,
		TransactionId: 7,
		Body: &pb.Envelope_HeartbeatRequest{
			HeartbeatRequest: &pb.HeartbeatRequest{NodeId: 1, Sequence: 2},
		},
	}
	wire, err := encode(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decode(wire)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !proto.Equal(original, got) {
		t.Fatal("codec round-trip mismatch")
	}
}
