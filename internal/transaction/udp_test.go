package transaction

import (
	"context"
	"errors"
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

func nodeFor(addr net.Addr) *registry.Node {
	ua := addr.(*net.UDPAddr)
	return &registry.Node{
		ID:      7,
		Type:    registry.TypeLogic,
		Address: ua.IP.String(),
		UDPPort: uint32(ua.Port),
		State:   registry.NodeStateActive,
		Services: []registry.Service{{
			ID: 1, Name: "logic", MessageTypes: []uint32{1001},
		}},
	}
}

func TestUDPRequestSuccess(t *testing.T) {
	t.Parallel()

	gw := newUDP(t)
	logic := newUDP(t)
	m := NewManager()
	svc, err := NewService(m, &stubRouter{node: nodeFor(logic.LocalAddr())}, gw, Config{Timeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Một Receive loop phía gateway → OnResponse. Service không gọi Receive.
	go func() {
		for {
			env, _, err := gw.Receive(ctx)
			if err != nil {
				return
			}
			_ = svc.OnResponse(env)
		}
	}()

	// Logic giả lập: nhận DATA_REQUEST, gửi DATA_RESPONSE.
	go func() {
		env, from, err := logic.Receive(ctx)
		if err != nil {
			return
		}
		if env.GetType() != pb.MessageType_MESSAGE_TYPE_DATA_REQUEST {
			return
		}
		resp := dataResp(env.GetTransactionId(), env.GetDataRequest().GetMessageId())
		resp.TraceId = "udp-ok"
		_ = logic.Send(ctx, resp, from)
	}()

	got, err := svc.Request(ctx, dataReq(40, 1001))
	if err != nil {
		t.Fatal(err)
	}
	if got.GetType() != pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE || got.GetTransactionId() != 40 {
		t.Fatalf("got %+v", got)
	}
}

func TestUDPTimeout(t *testing.T) {
	t.Parallel()

	gw := newUDP(t)
	logic := newUDP(t)
	m := NewManager()
	svc, err := NewService(m, &stubRouter{node: nodeFor(logic.LocalAddr())}, gw, Config{Timeout: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close() })

	_, err = svc.Request(context.Background(), dataReq(41, 1001))
	if !errors.Is(err, ErrTransactionTimeout) {
		t.Fatalf("err = %v", err)
	}
	if err := m.Complete(41, dataResp(41, 1001)); !errors.Is(err, ErrUnknownTransaction) {
		t.Fatalf("pending not cleaned: %v", err)
	}
}
