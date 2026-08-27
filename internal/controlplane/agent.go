package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/haison65/logic-gateway/internal/registry"
	"go.uber.org/zap"
)

// RegisterBody là payload HTTP control-plane (JSON).
type RegisterBody struct {
	NodeID     uint32  `json:"node_id"`
	InstanceID string  `json:"instance_id"`
	Name       string  `json:"name"`
	Type       string  `json:"type"`
	Group      string  `json:"group"`
	Address    string  `json:"address"`
	UDPPort    uint32  `json:"udp_port"`
	HTTPPort   uint32  `json:"http_port"`
	CPUCores   float64 `json:"cpu_cores"`
	Memory     string  `json:"memory"` // human, parsed by master
	MemoryBytes uint64 `json:"memory_bytes,omitempty"`
}

// HeartbeatBody payload HB định kỳ.
type HeartbeatBody struct {
	NodeID            uint32 `json:"node_id"`
	Load              uint32 `json:"load"`
	ActiveTransaction uint32 `json:"active_transaction"`
}

// NodeView JSON status.
type NodeView struct {
	NodeID            uint32  `json:"node_id"`
	InstanceID        string  `json:"instance_id"`
	Name              string  `json:"name"`
	Type              string  `json:"type"`
	Group             string  `json:"group"`
	Address           string  `json:"address"`
	UDPPort           uint32  `json:"udp_port"`
	HTTPPort          uint32  `json:"http_port"`
	State             string  `json:"state"`
	LastHeartbeatUnix int64   `json:"last_heartbeat_unix"`
	Load              uint32  `json:"load"`
	ActiveTransaction uint32  `json:"active_transaction"`
	CPULimitCores     float64 `json:"cpu_limit_cores"`
	MemoryLimitBytes  uint64  `json:"memory_limit_bytes"`
}

// ParseType map string → registry.Type.
func ParseType(s string) (registry.Type, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "_")
	switch s {
	case "http2gw", "http2_server", "http2server":
		return registry.TypeHTTP2GW, nil
	case "logic":
		return registry.TypeLogic, nil
	case "master":
		return registry.TypeMaster, nil
	case "http2_client", "http2client":
		return registry.TypeHTTP2Client, nil
	case "performance", "perf":
		return registry.TypePerformance, nil
	default:
		return registry.TypeUnspecified, fmt.Errorf("unknown node type %q", s)
	}
}

// Agent báo cáo Register + Heartbeat tới Master qua HTTP.
type Agent struct {
	MasterURL string
	Body      RegisterBody
	Interval  time.Duration
	Client    *http.Client
	Log       *zap.Logger
}

// Run register (retry) rồi heartbeat đến khi ctx hủy; cố Leave khi thoát.
func (a *Agent) Run(ctx context.Context) error {
	if a == nil || strings.TrimSpace(a.MasterURL) == "" {
		return nil
	}
	if a.Client == nil {
		a.Client = &http.Client{Timeout: 3 * time.Second}
	}
	if a.Interval <= 0 {
		a.Interval = time.Second
	}
	if a.Log == nil {
		a.Log = zap.NewNop()
	}
	base := strings.TrimRight(a.MasterURL, "/")

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := a.postJSON(ctx, base+"/v1/register", a.Body); err != nil {
			a.Log.Warn("controlplane register thất bại", zap.Error(err), zap.String("master", base))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(a.Interval):
				continue
			}
		}
		a.Log.Info("controlplane đã đăng ký",
			zap.Uint32("node_id", a.Body.NodeID),
			zap.String("name", a.Body.Name),
			zap.String("type", a.Body.Type),
		)
		break
	}

	ticker := time.NewTicker(a.Interval)
	defer ticker.Stop()
	defer func() {
		leaveCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = a.postJSON(leaveCtx, fmt.Sprintf("%s/v1/nodes/%d/leave", base, a.Body.NodeID), nil)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			hb := HeartbeatBody{NodeID: a.Body.NodeID}
			if err := a.postJSON(ctx, base+"/v1/heartbeat", hb); err != nil {
				a.Log.Debug("controlplane heartbeat thất bại", zap.Error(err))
			}
		}
	}
}

func (a *Agent) postJSON(ctx context.Context, url string, body any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// ToView map registry.Node → NodeView.
func ToView(n *registry.Node) NodeView {
	if n == nil {
		return NodeView{}
	}
	name := n.Name
	if name == "" {
		name = n.InstanceID
	}
	var hb int64
	if !n.LastHeartbeatAt.IsZero() {
		hb = n.LastHeartbeatAt.Unix()
	}
	return NodeView{
		NodeID:            n.ID,
		InstanceID:        n.InstanceID,
		Name:              name,
		Type:              n.Type.String(),
		Group:             n.Group,
		Address:           n.Address,
		UDPPort:           n.UDPPort,
		HTTPPort:          n.HTTPPort,
		State:             n.State.String(),
		LastHeartbeatUnix: hb,
		Load:              n.Load,
		ActiveTransaction: n.ActiveTransaction,
		CPULimitCores:     n.Resource.CPULimitCores,
		MemoryLimitBytes:  n.Resource.MemoryLimitBytes,
	}
}
