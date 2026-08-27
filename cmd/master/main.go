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
	"github.com/haison65/logic-gateway/internal/master"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "configs/local/master.dev.yaml", "đường dẫn YAML Master")
	debug := flag.Bool("debug", true, "log debug")
	flag.Parse()

	log := logger.New(*debug, "master")
	defer func() { _ = log.Sync() }()
	zap.ReplaceGlobals(log)

	cfg, err := config.LoadMaster(*configPath)
	if err != nil {
		log.Error("không đọc được cấu hình", zap.Error(err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("master khởi động",
		zap.Uint32("node_id", cfg.Node.NodeID),
		zap.String("name", cfg.Node.Name),
		zap.Int("http_port", cfg.HTTP.Port),
	)

	if err := master.Run(ctx, cfg, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("master dừng vì lỗi", zap.Error(err))
		os.Exit(1)
	}
	log.Info("master đã dừng")
}
