package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/haison65/logic-gateway/internal/config"
	"github.com/haison65/logic-gateway/internal/gateway"
	"github.com/haison65/logic-gateway/internal/logger"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "configs/http2gw.yaml", "đường dẫn YAML cấu hình")
	debug := flag.Bool("debug", true, "in log debug xử lý request")
	flag.Parse()

	log := logger.New(*debug, "http2gw")
	defer func() { _ = log.Sync() }()
	zap.ReplaceGlobals(log)

	cfg, err := config.LoadHTTP2GW(*configPath)
	if err != nil {
		log.Error("không đọc được cấu hình", zap.Error(err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("http2gw khởi động",
		zap.Uint32("node_id", cfg.Node.NodeID),
		zap.String("instance_id", cfg.Node.InstanceID),
		zap.String("udp_listen", cfg.UDP.Listen),
		zap.Int("udp_port", cfg.UDP.Port),
		zap.Int("http_port", cfg.HTTP.Port),
		zap.String("routing", cfg.StrategyName()),
		zap.Bool("debug", *debug),
	)

	if err := gateway.Run(ctx, cfg, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("http2gw dừng vì lỗi", zap.Error(err))
		os.Exit(1)
	}
	log.Info("http2gw đã dừng")
}
