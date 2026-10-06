package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
	fraudv1 "github.com/wreckx-in-scene/GoWallet/gen/fraud/v1"
	paymentv1 "github.com/wreckx-in-scene/GoWallet/gen/payment/v1"
	walletv1 "github.com/wreckx-in-scene/GoWallet/gen/wallet/v1"
	"github.com/wreckx-in-scene/GoWallet/internal/payment"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/grpcserver"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/logger"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/metrics"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/outbox"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/postgres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "payment:", err)
		os.Exit(1)
	}
}

func run() error {
	if err := config.LoadDotEnv(); err != nil {
		return err
	}
	cfg, err := payment.LoadConfig()
	if err != nil {
		return err
	}
	log, err := logger.New(cfg.Env, cfg.LogLevel)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := metrics.Serve(ctx, log, "payment"); err != nil {
		return err
	}

	pool, err := postgres.NewPool(ctx, cfg.DBURL)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()
	log.Info("database connected")

	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	walletConn, err := grpc.NewClient(cfg.WalletAddr, creds)
	if err != nil {
		return fmt.Errorf("wallet client: %w", err)
	}
	defer walletConn.Close()
	fraudConn, err := grpc.NewClient(cfg.FraudAddr, creds)
	if err != nil {
		return fmt.Errorf("fraud client: %w", err)
	}
	defer fraudConn.Close()

	kafka, err := kgo.NewClient(kgo.SeedBrokers(cfg.KafkaBrokers...))
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer kafka.Close()

	svc := payment.NewService(
		payment.NewStore(pool),
		walletv1.NewWalletServiceClient(walletConn),
		fraudv1.NewFraudServiceClient(fraudConn),
		log,
	)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); outbox.NewRelay(pool, kafka, log).Run(ctx) }()
	go func() { defer wg.Done(); svc.RunRecovery(ctx) }()

	srv := grpcserver.New(grpcserver.Options{
		Name:       "payment",
		Port:       cfg.GRPCPort,
		Log:        log,
		DB:         pool,
		Reflection: cfg.Env == "dev",
		Register: func(s *grpc.Server) {
			paymentv1.RegisterPaymentServiceServer(s, payment.NewHandler(svc, log))
		},
	})
	srvErr := srv.Run(ctx)
	stop()
	wg.Wait()
	return srvErr
}
