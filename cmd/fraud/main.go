package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	fraudv1 "github.com/wreckx-in-scene/GoWallet/gen/fraud/v1"
	"github.com/wreckx-in-scene/GoWallet/internal/fraud"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/grpcserver"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/logger"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/metrics"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/postgres"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fraud:", err)
		os.Exit(1)
	}
}

func run() error {
	if err := config.LoadDotEnv(); err != nil {
		return err
	}
	cfg, err := fraud.LoadConfig()
	if err != nil {
		return err
	}
	log, err := logger.New(cfg.Env, cfg.LogLevel)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := metrics.Serve(ctx, log, "fraud"); err != nil {
		return err
	}

	pool, err := postgres.NewPool(ctx, cfg.DBURL)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()
	log.Info("database connected")

	store := fraud.NewStore(pool, fraud.Rules{MaxAmountPaise: cfg.MaxAmountPaise, MaxPerMinute: cfg.MaxPerMinute})
	srv := grpcserver.New(grpcserver.Options{
		Name:       "fraud",
		Port:       cfg.GRPCPort,
		Log:        log,
		DB:         pool,
		Reflection: cfg.Env == "dev",
		Register: func(s *grpc.Server) {
			fraudv1.RegisterFraudServiceServer(s, fraud.NewHandler(store, log))
		},
	})
	return srv.Run(ctx)
}
