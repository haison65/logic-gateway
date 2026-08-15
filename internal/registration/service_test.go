package registration

import (
	"errors"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/registry"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

type fakeRegistry struct {
	nodes     map[uint32]*registry.Node
	registerE error
}

func newFake() *fakeRegistry {
	return &fakeRegistry{nodes: make(map[uint32]*registry.Node)}
}

func (f *fakeRegistry) Register(node *registry.Node) error {
	if f.registerE != nil {
		return f.registerE
	}
	copied := *node
	f.nodes[node.ID] = &copied
	return nil
}

func (f *fakeRegistry) Get(nodeID uint32) (*registry.Node, bool) {
	n, ok := f.nodes[nodeID]
	if !ok {
		return nil, false
	}
	copied := *n
	return &copied, true
}

func (f *fakeRegistry) Remove(nodeID uint32) error {
	if _, ok := f.nodes[nodeID]; !ok {
		return registry.ErrNotFound
	}
	delete(f.nodes, nodeID)
	return nil
}

func (f *fakeRegistry) List() []*registry.Node {
	out := make([]*registry.Node, 0, len(f.nodes))
	for _, n := range f.nodes {
		copied := *n
		out = append(out, &copied)
	}
	return out
}

func (f *fakeRegistry) ApplyHeartbeat(nodeID uint32, at time.Time, transition func(registry.NodeState) (registry.NodeState, error)) (*registry.Node, error) {
	n, ok := f.nodes[nodeID]
	if !ok {
		return nil, registry.ErrNotFound
	}
	next, err := transition(n.State)
	if err != nil {
		copied := *n
		return &copied, err
	}
	n.State = next
	n.LastHeartbeatAt = at
	copied := *n
	return &copied, nil
}

func (f *fakeRegistry) ApplyTimeouts(now time.Time, transition func(state registry.NodeState, lastHeartbeat time.Time, now time.Time) (registry.NodeState, bool)) []*registry.Node {
	var changed []*registry.Node
	for _, n := range f.nodes {
		next, ok := transition(n.State, n.LastHeartbeatAt, now)
		if !ok || next == n.State {
			continue
		}
		n.State = next
		copied := *n
		changed = append(changed, &copied)
	}
	return changed
}

func (f *fakeRegistry) SetRuntime(nodeID uint32, load, activeTransaction uint32) error {
	n, ok := f.nodes[nodeID]
	if !ok {
		return registry.ErrNotFound
	}
	n.Load = load
	n.ActiveTransaction = activeTransaction
	return nil
}

func validRequest() *pb.RegisterRequest {
	return &pb.RegisterRequest{
		NodeId:     42,
		NodeType:   pb.NodeType_NODE_TYPE_LOGIC,
		InstanceId: "logic-1",
		Ip:         "127.0.0.1",
		Port:       9100,
		Services: []*pb.ServiceCapability{{
			ServiceId:    100,
			ServiceName:  "call",
			MessageTypes: []uint32{1001, 1002, 1003},
		}},
	}
}

func TestRegisterValid(t *testing.T) {
	t.Parallel()

	reg := newFake()
	svc := NewService(reg)
	resp, err := svc.Register(validRequest())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if !resp.GetAccepted() || resp.GetAssignedNodeId() != 42 {
		t.Fatalf("response = %+v", resp)
	}

	node, ok := reg.Get(42)
	if !ok {
		t.Fatal("node not stored")
	}
	if node.InstanceID != "logic-1" || node.Address != "127.0.0.1" || node.UDPPort != 9100 {
		t.Fatalf("mapped node = %+v", node)
	}
	if node.Type != registry.TypeLogic {
		t.Fatalf("type = %v", node.Type)
	}
	if len(node.Services) != 1 || node.Services[0].ID != 100 || node.Services[0].Name != "call" {
		t.Fatalf("services = %+v", node.Services)
	}
}

func TestRegisterInvalid(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		req  *pb.RegisterRequest
	}{
		{name: "nil", req: nil},
		{name: "zero node_id", req: &pb.RegisterRequest{
			NodeId: 0, NodeType: pb.NodeType_NODE_TYPE_LOGIC, InstanceId: "a", Ip: "127.0.0.1", Port: 1,
		}},
		{name: "unknown type", req: &pb.RegisterRequest{
			NodeId: 1, NodeType: pb.NodeType_NODE_TYPE_UNKNOWN, InstanceId: "a", Ip: "127.0.0.1", Port: 1,
		}},
		{name: "empty instance", req: &pb.RegisterRequest{
			NodeId: 1, NodeType: pb.NodeType_NODE_TYPE_LOGIC, InstanceId: "  ", Ip: "127.0.0.1", Port: 1,
		}},
		{name: "bad ip", req: &pb.RegisterRequest{
			NodeId: 1, NodeType: pb.NodeType_NODE_TYPE_LOGIC, InstanceId: "a", Ip: "not-an-ip", Port: 1,
		}},
		{name: "port 0", req: &pb.RegisterRequest{
			NodeId: 1, NodeType: pb.NodeType_NODE_TYPE_LOGIC, InstanceId: "a", Ip: "127.0.0.1", Port: 0,
		}},
		{name: "zero service_id", req: &pb.RegisterRequest{
			NodeId: 1, NodeType: pb.NodeType_NODE_TYPE_LOGIC, InstanceId: "a", Ip: "127.0.0.1", Port: 1,
			Services: []*pb.ServiceCapability{{ServiceId: 0, ServiceName: "x"}},
		}},
	}

	svc := NewService(newFake())
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp, err := svc.Register(tc.req)
			if err == nil {
				t.Fatal("expected error")
			}
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("err = %v, want ErrInvalidRequest", err)
			}
			if resp == nil || resp.GetAccepted() {
				t.Fatalf("expected rejected response, got %+v", resp)
			}
		})
	}
}

func TestRegisterDuplicateUpdates(t *testing.T) {
	t.Parallel()

	reg := registry.NewMemory()
	svc := NewService(reg)
	first := validRequest()
	if _, err := svc.Register(first); err != nil {
		t.Fatal(err)
	}

	second := validRequest()
	second.Ip = "10.1.2.3"
	second.Port = 9200
	second.InstanceId = "logic-1b"
	resp, err := svc.Register(second)
	if err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("response = %+v", resp)
	}
	if got := len(reg.List()); got != 1 {
		t.Fatalf("len(List) = %d, want 1", got)
	}
	node, _ := reg.Get(42)
	if node.Address != "10.1.2.3" || node.UDPPort != 9200 || node.InstanceID != "logic-1b" {
		t.Fatalf("updated node = %+v", node)
	}
}

func TestRegisterRegistryError(t *testing.T) {
	t.Parallel()

	reg := newFake()
	reg.registerE = errors.New("store failed")
	svc := NewService(reg)
	resp, err := svc.Register(validRequest())
	if err == nil {
		t.Fatal("expected error")
	}
	if resp == nil || resp.GetAccepted() {
		t.Fatalf("expected rejected response, got %+v", resp)
	}
}

func TestValidateEmptyServicesOK(t *testing.T) {
	t.Parallel()

	req := validRequest()
	req.Services = nil
	if err := Validate(req); err != nil {
		t.Fatalf("empty services should be allowed: %v", err)
	}
}
