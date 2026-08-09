package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Zhiruosama/Email-Service/internal/bootstrap"
)

func main() {
	os.Exit(run())
}

func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	config, err := bootstrap.LoadConfig()
	if err != nil {
		logger.Error("service configuration rejected", "error", err)
		return 1
	}
	app, err := bootstrap.NewApp(ctx, config, logger)
	if err != nil {
		if ctx.Err() != nil {
			logger.Info("service startup cancelled")
			return 0
		}
		logger.Error("service startup failed", "error", err)
		return 1
	}
	if err := app.Run(ctx); err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return 0
		}
		logger.Error("service runtime failed", "error", err)
		return 1
	}
	return 0
}
