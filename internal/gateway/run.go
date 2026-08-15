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
	"github.com/haison65/logic-gateway/internal/dispatch"
	"github.com/haison65/logic-gateway/internal/heartbeat"
	"github.com/haison65/logic-gateway/internal/httpsrv"
	"github.com/haison65/logic-gateway/internal/logger"
	"github.com/haison65/logic-gateway/internal/metrics"
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
	errCh := make(chan error, 3)

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

// registerSelf đưa HTTP2GW vào Registry local.
// §6 yêu cầu "gửi REGISTER" khi start nhưng không nêu đích UDP (Logic gửi tới gateway).
// Không invent peer/mesh: đăng ký semantic NODE_TYPE_HTTP2GW vào cùng Registry.
func registerSelf(reg *registration.Service, cfg config.HTTP2GW, conn udp.Transport) error {
	ip := strings.TrimSpace(cfg.Node.IP)
	if ip == "" || ip == "0.0.0.0" {
		ip = strings.TrimSpace(cfg.UDP.Listen)
	}
	if ip == "" || ip == "0.0.0.0" {
		ip = "127.0.0.1"
	}
	port := uint32(cfg.UDP.Port)
	if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok && addr != nil && addr.Port > 0 {
		port = uint32(addr.Port)
	}
	resp, err := reg.Register(&pb.RegisterRequest{
		NodeId:     cfg.Node.NodeID,
		NodeType:   pb.NodeType_NODE_TYPE_HTTP2GW,
		InstanceId: cfg.Node.InstanceID,
		Ip:         ip,
		Port:       port,
	})
	if err != nil {
		return err
	}
	if resp == nil || !resp.GetAccepted() {
		return fmt.Errorf("http2gw register rejected")
	}
	return nil
}
