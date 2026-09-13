package httpsrv

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strings"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// StartPprofListen mở HTTP/2 h2c server riêng cho net/http/pprof
// (không dùng Read/WriteTimeout ngắn của API data path).
// addr rỗng → no-op. Ví dụ: "0.0.0.0:6060" hoặc ":6060".
// Bật block/mutex profile để soi contention dưới load (P0).
func StartPprofListen(ctx context.Context, addr string, log *zap.Logger) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil
	}
	if log == nil {
		log = zap.NewNop()
	}

	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))

	// Cùng kiểu MetricServer h2c: Handler = h2c.NewHandler(router, &http2.Server{}).
	// Không đặt Read/WriteTimeout ngắn — CPU profile mặc định 30s.
	srv := &http.Server{
		Addr:              addr,
		Handler:           h2c.NewHandler(mux, &http2.Server{}),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Info("pprof MetricServer (h2c) đang lắng nghe",
		zap.String("addr", ln.Addr().String()),
		zap.String("index", "http://"+ln.Addr().String()+"/debug/pprof/"),
		zap.String("cpu", "http://"+ln.Addr().String()+"/debug/pprof/profile?seconds=30"),
	)

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	go func() {
		err := srv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Warn("pprof server dừng", zap.Error(err))
		}
	}()
	return nil
}
