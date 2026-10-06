package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/wreckx-in-scene/GoWallet/internal/notification"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/grpcserver"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/kafkaconsumer"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/logger"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/postgres"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "notification:", err)
		os.Exit(1)
	}
}

func run() error {
	if err := config.LoadDotEnv(); err != nil {
		return err
	}
	cfg, err := notification.LoadConfig()
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

	producer, err := kgo.NewClient(kgo.SeedBrokers(cfg.KafkaBrokers...))
	if err != nil {
		return fmt.Errorf("create kafka producer: %w", err)
	}
	defer producer.Close()

	consumer, err := kafkaconsumer.New(cfg.KafkaBrokers, "notification", "payment.events", log)
	if err != nil {
		return err
	}
	defer consumer.Close()

	notifier := notification.NewNotifier(
		notification.NewStore(pool),
		notification.NewLogSender(log, cfg.FailPercent),
		producer, log,
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); consumer.Run(ctx, notifier.Handle) }()

	srv := grpcserver.New(grpcserver.Options{
		Name:       "notification",
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
