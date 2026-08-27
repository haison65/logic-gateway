package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/haison65/logic-gateway/internal/config"
	"github.com/haison65/logic-gateway/internal/controlplane"
	"github.com/haison65/logic-gateway/internal/dispatch"
	"github.com/haison65/logic-gateway/internal/heartbeat"
	"github.com/haison65/logic-gateway/internal/httpsrv"
	"github.com/haison65/logic-gateway/internal/logger"
	"github.com/haison65/logic-gateway/internal/metrics"
	"github.com/haison65/logic-gateway/internal/netaddr"
	"github.com/haison65/logic-gateway/internal/outbound"
	"github.com/haison65/logic-gateway/internal/registration"
	"github.com/haison65/logic-gateway/internal/registry"
	"github.com/haison65/logic-gateway/internal/router"
	"github.com/haison65/logic-gateway/internal/transaction"
	"github.com/haison65/logic-gateway/internal/transport/udp"
	pb "github.com/haison65/logic-gateway/proto/gen/go"
	"go.uber.org/zap"
)

// Run khởi động UDP, dispatcher, heartbeat monitor và HTTP/2 đến khi ctx hủy.
func Run(ctx context.Context, cfg config.HTTP2GW, log *zap.Logger) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	log = logger.OrNop(log)

	conn, err := udp.New(udp.Config{
		ListenHost: cfg.UDP.Listen,
		ListenPort: cfg.UDP.Port,
	})
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	log.Info("http2gw đang lắng nghe UDP", zap.Stringer("addr", conn.LocalAddr()))

	met := metrics.New()
	regStore := registry.NewMemory()
	regSvc := registration.NewService(regStore)
	if err := registerSelf(regSvc, cfg, conn); err != nil {
		log.Warn("http2gw tự đăng ký thất bại", zap.Error(err))
	}
	hbSvc := heartbeat.NewService(regStore, nil)
	mon, err := heartbeat.NewMonitor(regStore, heartbeat.Config{
		Interval:       cfg.Heartbeat.Interval.Duration(),
		SuspectTimeout: cfg.Heartbeat.Timeout.Duration(),
		DeadTimeout:    cfg.Heartbeat.DeadTimeout.Duration(),
	}, nil)
	if err != nil {
		return err
	}
	mon.OnTimeout = func(n int) { met.AddHeartbeatTimeout(int64(n)) }

	hbSender, err := heartbeat.NewSender(
		cfg.Node.NodeID,
		regStore,
		conn,
		cfg.Heartbeat.Interval.Duration(),
		nil,
		log.Named("heartbeat.outbound"),
	)
	if err != nil {
		return err
	}

	rt := router.New(regStore, newStrategy(cfg.StrategyName()))
	txMgr := transaction.NewManager()
	txSvc, err := transaction.NewService(txMgr, rt, conn, transaction.Config{
		Timeout: cfg.Transaction.Timeout.Duration(),
	})
	if err != nil {
		return err
	}
	defer func() { _ = txSvc.Close() }()

	ob := outbound.New(cfg.HTTP.Remote, cfg.Transaction.Timeout.Duration(), nil)
	disp, err := dispatch.New(cfg.Node.NodeID, conn, regSvc, hbSvc, txSvc, log.Named("dispatch"), ob, met)
	if err != nil {
		return err
	}
	httpSrv, err := httpsrv.New(httpsrv.Config{
		NodeID:  cfg.Node.NodeID,
		Listen:  cfg.HTTP.Listen,
		Port:    cfg.HTTP.Port,
		TLSCert: cfg.HTTP.TLSCert,
		TLSKey:  cfg.HTTP.TLSKey,
	}, txSvc, log.Named("httpsrv"), met)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 6)

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				by := map[string]int{}
				for _, n := range regStore.List() {
					if n == nil || n.Type != registry.TypeLogic {
						continue
					}
					by[n.State.String()]++
				}
				met.ObserveLogicRegistry(by)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := mon.Run(runCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("heartbeat monitor: %w", err)
			cancel()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := hbSender.Run(runCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("heartbeat sender: %w", err)
			cancel()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := disp.Run(runCtx)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, udp.ErrTransportClosed) {
			errCh <- fmt.Errorf("dispatcher: %w", err)
			cancel()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := httpSrv.Serve()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http: %w", err)
			cancel()
		}
	}()

	if strings.TrimSpace(cfg.MasterURL) != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			agent := &controlplane.Agent{
				MasterURL: cfg.MasterURL,
				Body:      controlplane.BodyFromHTTP2GW(cfg),
				Interval:  cfg.Heartbeat.Interval.Duration(),
				Log:       log.Named("controlplane"),
			}
			if agent.Body.Address == "" || netaddr.IsUnspecified(agent.Body.Address) {
				agent.Body.Address = "127.0.0.1"
			}
			_ = agent.Run(runCtx)
		}()
	}

	var runErr error
	select {
	case <-runCtx.Done():
		runErr = ctx.Err()
	case err := <-errCh:
		runErr = err
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	_ = httpSrv.Shutdown(shutCtx)
	_ = conn.Close()
	_ = txSvc.Close()
	cancel()
	wg.Wait()
	return runErr
}

func newStrategy(name string) router.Strategy {
	if name == "round_robin" {
		return router.NewRoundRobin()
	}
	return router.NewConsistentHash(0)
}

// registerSelf đưa HTTP2GW vào Registry local khi start (§6).
// Design yêu cầu gửi REGISTER + identity + capability + message_types.
// Không nêu đích UDP peer (Logic mới gửi REGISTER tới gateway) → đăng ký semantic
// NODE_TYPE_HTTP2GW vào cùng Registry, kèm Services từ YAML (không invent mesh).
func registerSelf(reg *registration.Service, cfg config.HTTP2GW, conn udp.Transport) error {
	host := strings.TrimSpace(cfg.Node.IP)
	if host == "" || netaddr.IsUnspecified(host) {
		host = strings.TrimSpace(cfg.UDP.Listen)
	}
	ip := "127.0.0.1"
	if host != "" && !netaddr.IsUnspecified(host) {
		resolved, err := netaddr.ResolveIP(context.Background(), host)
		if err != nil {
			return fmt.Errorf("http2gw advertise: %w", err)
		}
		ip = resolved.String()
	}
	port := uint32(cfg.UDP.Port)
	if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok && addr != nil && addr.Port > 0 {
		port = uint32(addr.Port)
	}
	services := make([]*pb.ServiceCapability, 0, len(cfg.Services))
	for _, svc := range cfg.Services {
		services = append(services, &pb.ServiceCapability{
			ServiceId:    svc.ServiceID,
			ServiceName:  svc.ServiceName,
			MessageTypes: append([]uint32(nil), svc.MessageTypes...),
		})
	}
	resp, err := reg.Register(&pb.RegisterRequest{
		NodeId:     cfg.Node.NodeID,
		NodeType:   pb.NodeType_NODE_TYPE_HTTP2GW,
		InstanceId: cfg.Node.InstanceID,
		Ip:         ip,
		Port:       port,
		Services:   services,
	})
	if err != nil {
		return err
	}
	if resp == nil || !resp.GetAccepted() {
		return fmt.Errorf("http2gw register rejected")
	}
	return nil
}
