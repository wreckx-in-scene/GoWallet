package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	walletv1 "github.com/wreckx-in-scene/GoWallet/gen/wallet/v1"
	"github.com/wreckx-in-scene/GoWallet/internal/ledger"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/grpcserver"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/logger"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/postgres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ledger:", err)
		os.Exit(1)
	}
}

func run() error {
	if err := config.LoadDotEnv(); err != nil {
		return err
	}
	cfg, err := ledger.LoadConfig()
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

	walletConn, err := grpc.NewClient(cfg.WalletAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("wallet client: %w", err)
	}
	defer walletConn.Close()

	consumer, err := ledger.NewConsumer(pool, cfg.KafkaBrokers, log)
	if err != nil {
		return err
	}
	defer consumer.Close()

	reconciler := ledger.NewReconciler(pool, walletv1.NewWalletServiceClient(walletConn), log, cfg.ReconcileEvery)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); consumer.Run(ctx) }()
	go func() { defer wg.Done(); reconciler.Run(ctx) }()

	// Health-only gRPC server (no business RPCs yet).
	srv := grpcserver.New(grpcserver.Options{
		Name:       "ledger",
		Port:       cfg.GRPCPort,
		Log:        log,
		DB:         pool,
		Reflection: cfg.Env == "dev",
		Register:   func(*grpc.Server) {},
	})
	srvErr := srv.Run(ctx)
	stop()
	wg.Wait()
	return srvErr
}
