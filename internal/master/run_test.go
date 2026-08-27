package master_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/haison65/logic-gateway/internal/config"
	"github.com/haison65/logic-gateway/internal/controlplane"
	"github.com/haison65/logic-gateway/internal/master"
	"go.uber.org/zap"
)

func TestMasterRegisterHeartbeatListLeave(t *testing.T) {
	t.Parallel()
	port := freeTCPPort(t)
	cfg := config.Master{
		Node: config.NodeMeta{
			NodeID: 1, InstanceID: "master-test", Name: "master", Type: "master", Group: "test",
		},
		HTTP: config.HTTP{Listen: "127.0.0.1", Port: port},
		Heartbeat: config.Heartbeat{
			Interval:    config.Duration(50 * time.Millisecond),
			Timeout:     config.Duration(500 * time.Millisecond),
			DeadTimeout: config.Duration(time.Second),
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- master.Run(ctx, cfg, zap.NewNop()) }()

	base := "http://127.0.0.1:" + strconv.Itoa(port)
	waitURL(t, base+"/healthz")

	agentCtx, agentCancel := context.WithCancel(context.Background())
	defer agentCancel()
	go func() {
		_ = (&controlplane.Agent{
			MasterURL: base,
			Body: controlplane.RegisterBody{
				NodeID: 2, InstanceID: "logic-1", Name: "logic-1", Type: "logic",
				Group: "test", Address: "127.0.0.1", UDPPort: 9100, CPUCores: 6, Memory: "8G",
			},
			Interval: 50 * time.Millisecond,
			Log:      zap.NewNop(),
		}).Run(agentCtx)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err := http.Get(base + "/v1/nodes")
		if err == nil {
			raw, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			var body struct {
				Count int                      `json:"count"`
				Nodes []controlplane.NodeView `json:"nodes"`
			}
			if resp.StatusCode == 200 && json.Unmarshal(raw, &body) == nil {
				logicOK := false
				for _, n := range body.Nodes {
					if n.NodeID == 2 && n.State == "active" && n.Type == "logic" {
						logicOK = true
					}
				}
				if body.Count >= 2 && logicOK {
					break
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for logic ACTIVE")
		}
		time.Sleep(40 * time.Millisecond)
	}

	agentCancel()
	resp, err := http.Post(base+"/v1/nodes/2/leave", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	resp, err = http.Get(base + "/v1/nodes")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	var body struct {
		Nodes []controlplane.NodeView `json:"nodes"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	for _, n := range body.Nodes {
		if n.NodeID == 2 {
			t.Fatalf("logic still listed: %+v", n)
		}
	}

	cancel()
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
		t.Fatal("master did not stop")
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitURL(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", url)
}
