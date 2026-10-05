package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
	authv1 "github.com/wreckx-in-scene/GoWallet/gen/auth/v1"
	"github.com/wreckx-in-scene/GoWallet/internal/auth"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/grpcserver"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/logger"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/outbox"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/postgres"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "auth:", err)
		os.Exit(1)
	}
}

func run() error {
	if err := config.LoadDotEnv(); err != nil {
		return err
	}
	cfg, err := auth.LoadConfig()
	if err != nil {
		return err
	}
	log, err := logger.New(cfg.Env, cfg.LogLevel)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DBURL)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()
	log.Info("database connected")

	kafka, err := kgo.NewClient(kgo.SeedBrokers(cfg.KafkaBrokers...))
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer kafka.Close()

	svc, err := auth.NewService(auth.NewStore(pool), cfg.KeyFile, cfg.AccessTTL, cfg.RefreshTTL, log)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); outbox.NewRelay(pool, kafka, log).Run(ctx) }()

	srv := grpcserver.New(grpcserver.Options{
		Name:       "auth",
		Port:       cfg.GRPCPort,
		Log:        log,
		DB:         pool,
		Reflection: cfg.Env == "dev",
		Register: func(s *grpc.Server) {
			authv1.RegisterAuthServiceServer(s, auth.NewHandler(svc, log))
		},
	})
	srvErr := srv.Run(ctx)
	stop()
	wg.Wait()
	return srvErr
}
