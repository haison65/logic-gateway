package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/haison65/logic-gateway/internal/config"
	"github.com/haison65/logic-gateway/internal/logger"
	"github.com/haison65/logic-gateway/internal/logicnode"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "configs/logic.yaml", "đường dẫn YAML cấu hình")
	debug := flag.Bool("debug", true, "in log debug xử lý request")
	flag.Parse()

	log := logger.New(*debug, "logic")
	defer func() { _ = log.Sync() }()
	zap.ReplaceGlobals(log)

	cfg, err := config.LoadLogic(*configPath)
	if err != nil {
		log.Error("không đọc được cấu hình", zap.Error(err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("logic khởi động",
		zap.Uint32("node_id", cfg.Node.NodeID),
		zap.String("instance_id", cfg.Node.InstanceID),
		zap.String("udp_listen", cfg.UDP.Listen),
		zap.Int("udp_port", cfg.UDP.Port),
		zap.String("gateway_host", cfg.Gateway.Host),
		zap.Int("gateway_port", cfg.Gateway.Port),
		zap.Bool("debug", *debug),
	)

	if err := logicnode.Run(ctx, cfg, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("logic dừng vì lỗi", zap.Error(err))
		os.Exit(1)
	}
	log.Info("logic đã dừng")
}
