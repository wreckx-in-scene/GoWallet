package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/logger"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/outbox"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/postgres"
	"github.com/wreckx-in-scene/GoWallet/internal/wallet"
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

	log.Debug("config loaded", "grpc_port", cfg.GRPCPort)
	log.Info("wallet starting", "env", cfg.Env, "grpc_port", cfg.GRPCPort)

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

	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		outbox.NewRelay(pool, kafka, log).Run(ctx)
	}()

	srvErr := wallet.NewServer(cfg, log, pool).Run(ctx)
	stop()      // cancel ctx even if Run failed early, so the relay stops too
	<-relayDone // wait until the relay is really done with the DB
	return srvErr
}
