package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/haison65/logic-gateway/internal/config"
	"github.com/haison65/logic-gateway/internal/controlplane"
	"github.com/haison65/logic-gateway/internal/heartbeat"
	"github.com/haison65/logic-gateway/internal/logger"
	"github.com/haison65/logic-gateway/internal/registry"
	"go.uber.org/zap"
)

// Run khởi động Master control-plane đến khi ctx hủy.
func Run(ctx context.Context, cfg config.Master, log *zap.Logger) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	log = logger.OrNop(log).Named("master")

	reg := registry.NewMemory()
	self := &registry.Node{
		ID:         cfg.Node.NodeID,
		InstanceID: cfg.Node.InstanceID,
		Name:       cfg.Node.Name,
		Type:       registry.TypeMaster,
		Group:      cfg.Node.Group,
		Address:    strings.TrimSpace(cfg.Node.IP),
		HTTPPort:   uint32(cfg.HTTP.Port),
		Resource: registry.ResourceInfo{
			CPULimitCores:    cfg.Resource.CPUCores,
			MemoryLimitBytes: cfg.Resource.MemoryBytes(),
		},
	}
	if self.Address == "" {
		self.Address = "127.0.0.1"
	}
	if err := reg.Register(self); err != nil {
		return err
	}
	// Self ACTIVE ngay (Master không HB qua HTTP chính nó).
	_, _ = reg.ApplyHeartbeat(self.ID, time.Now(), heartbeat.NextOnHeartbeat)

	monCfg := heartbeat.Config{
		Interval:       cfg.Heartbeat.Interval.Duration(),
		SuspectTimeout: cfg.Heartbeat.Timeout.Duration(),
		DeadTimeout:    cfg.Heartbeat.DeadTimeout.Duration(),
	}
	srv := &httpServer{
		cfg: cfg,
		reg: reg,
		log: log,
		mon: monCfg,
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.runMonitor(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
			cancel()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.serve(runCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			cancel()
		}
	}()

	var runErr error
	select {
	case <-runCtx.Done():
		runErr = ctx.Err()
	case err := <-errCh:
		runErr = err
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutCancel()
	_ = srv.shutdown(shutCtx)
	cancel()
	wg.Wait()
	return runErr
}

type httpServer struct {
	cfg    config.Master
	reg    *registry.Memory
	log    *zap.Logger
	mon    heartbeat.Config
	http   *http.Server
	mu     sync.Mutex
	ln     net.Listener
}

func (s *httpServer) serve(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.HandleFunc("GET /v1/nodes", s.handleListNodes)
	mux.HandleFunc("POST /v1/register", s.handleRegister)
	mux.HandleFunc("POST /v1/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("POST /v1/nodes/{id}/leave", s.handleLeave)

	addr := net.JoinHostPort(s.cfg.HTTP.Listen, strconv.Itoa(s.cfg.HTTP.Port))
	s.http = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen master: %w", err)
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	s.log.Info("master đang lắng nghe HTTP", zap.String("addr", ln.Addr().String()))

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.http.Shutdown(shutCtx)
	}()
	return s.http.Serve(ln)
}

func (s *httpServer) shutdown(ctx context.Context) error {
	if s.http == nil {
		return nil
	}
	return s.http.Shutdown(ctx)
}

func (s *httpServer) runMonitor(ctx context.Context) error {
	ticker := time.NewTicker(s.mon.Interval)
	defer ticker.Stop()
	s.checkTimeouts()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Master tự refresh heartbeat để không tự DEAD.
			_, _ = s.reg.ApplyHeartbeat(s.cfg.Node.NodeID, time.Now(), heartbeat.NextOnHeartbeat)
			s.checkTimeouts()
		}
	}
}

func (s *httpServer) checkTimeouts() {
	now := time.Now()
	changed := s.reg.ApplyTimeoutsExcept(now, func(n *registry.Node) bool {
		return n != nil && n.Type == registry.TypeMaster
	}, func(state registry.NodeState, last time.Time, now time.Time) (registry.NodeState, bool) {
		elapsed := now.Sub(last)
		if elapsed < 0 {
			elapsed = 0
		}
		return heartbeat.NextOnTimeout(state, elapsed, s.mon.SuspectTimeout, s.mon.DeadTimeout)
	})
	for _, n := range changed {
		s.log.Warn("controlplane node state đổi",
			zap.Uint32("node_id", n.ID),
			zap.String("name", n.Name),
			zap.String("type", n.Type.String()),
			zap.String("state", n.State.String()),
		)
	}
}

func (s *httpServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "role": "master"})
}

func (s *httpServer) handleReady(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *httpServer) handleListNodes(w http.ResponseWriter, _ *http.Request) {
	list := s.reg.List()
	out := make([]controlplane.NodeView, 0, len(list))
	for _, n := range list {
		out = append(out, controlplane.ToView(n))
	}
	type view struct {
		Level string                  `json:"level"`
		Msg   string                  `json:"msg"`
		Count int                     `json:"count"`
		Nodes []controlplane.NodeView `json:"nodes"`
	}
	writeJSONPretty(w, http.StatusOK, view{
		Level: "info",
		Msg:   "nodes_inventory",
		Count: len(out),
		Nodes: out,
	})
}

func (s *httpServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	var body controlplane.RegisterBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	typ, err := controlplane.ParseType(body.Type)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if body.NodeID == 0 || strings.TrimSpace(body.InstanceID) == "" {
		http.Error(w, "node_id and instance_id required", http.StatusBadRequest)
		return
	}
	mem := body.MemoryBytes
	if mem == 0 && body.Memory != "" {
		mem, _ = config.ParseMemoryBytes(body.Memory)
	}
	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = body.InstanceID
	}
	addr := strings.TrimSpace(body.Address)
	if addr == "" {
		addr = "127.0.0.1"
	}
	node := &registry.Node{
		ID:         body.NodeID,
		InstanceID: strings.TrimSpace(body.InstanceID),
		Name:       name,
		Type:       typ,
		Group:      strings.TrimSpace(body.Group),
		Address:    addr,
		UDPPort:    body.UDPPort,
		HTTPPort:   body.HTTPPort,
		Resource: registry.ResourceInfo{
			CPULimitCores:    body.CPUCores,
			MemoryLimitBytes: mem,
		},
	}
	if err := s.reg.Register(node); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = s.reg.ApplyHeartbeat(node.ID, time.Now(), heartbeat.NextOnHeartbeat)
	s.log.Info("node registered",
		zap.Uint32("node_id", node.ID),
		zap.String("name", node.Name),
		zap.String("type", node.Type.String()),
	)
	writeJSON(w, http.StatusOK, map[string]any{"accepted": true, "node": controlplane.ToView(node)})
}

func (s *httpServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var body controlplane.HeartbeatBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.NodeID == 0 {
		http.Error(w, "node_id required", http.StatusBadRequest)
		return
	}
	_, err := s.reg.ApplyHeartbeat(body.NodeID, time.Now(), heartbeat.NextOnHeartbeat)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			http.Error(w, "unknown node", http.StatusNotFound)
			return
		}
		if errors.Is(err, heartbeat.ErrDeadNode) {
			http.Error(w, "dead node; re-register required", http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = s.reg.SetRuntime(body.NodeID, body.Load, body.ActiveTransaction)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *httpServer) handleLeave(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id64, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil || id64 == 0 {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	id := uint32(id64)
	if id == s.cfg.Node.NodeID {
		http.Error(w, "cannot leave master via this endpoint", http.StatusBadRequest)
		return
	}
	_ = s.reg.Remove(id)
	s.log.Info("node left", zap.Uint32("node_id", id))
	writeJSON(w, http.StatusOK, map[string]string{"status": "left"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONPretty(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
