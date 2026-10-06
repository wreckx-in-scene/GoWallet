package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
	walletv1 "github.com/wreckx-in-scene/GoWallet/gen/wallet/v1"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/grpcserver"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/logger"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/metrics"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/outbox"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/postgres"
	"github.com/wreckx-in-scene/GoWallet/internal/wallet"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "wallet:", err)
		os.Exit(1)
	}
}

func run() error {
	if err := config.LoadDotEnv(); err != nil {
		return err
	}
	cfg, err := wallet.LoadConfig()
	if err != nil {
		return err
	}
	log, err := logger.New(cfg.Env, cfg.LogLevel)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := metrics.Serve(ctx, log, "wallet"); err != nil {
		return err
	}

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

	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		outbox.NewRelay(pool, kafka, log).Run(ctx)
	}()

	store := wallet.NewStore(pool)
	srv := grpcserver.New(grpcserver.Options{
		Name:       "wallet",
		Port:       cfg.GRPCPort,
		Log:        log,
		DB:         pool,
		Reflection: cfg.Env == "dev",
		Register: func(s *grpc.Server) {
			walletv1.RegisterWalletServiceServer(s, wallet.NewHandler(store, log))
		},
	})
	srvErr := srv.Run(ctx)
	stop()
	<-relayDone
	return srvErr
}
