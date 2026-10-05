package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	userv1 "github.com/wreckx-in-scene/GoWallet/gen/user/v1"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/grpcserver"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/kafkaconsumer"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/logger"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/postgres"
	"github.com/wreckx-in-scene/GoWallet/internal/user"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "user:", err)
		os.Exit(1)
	}
}

func run() error {
	if err := config.LoadDotEnv(); err != nil {
		return err
	}
	cfg, err := user.LoadConfig()
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

	store := user.NewStore(pool)
	consumer, err := kafkaconsumer.New(cfg.KafkaBrokers, "user", "user.events", log)
	if err != nil {
		return err
	}
	defer consumer.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); consumer.Run(ctx, user.NewProjector(store, log).Handle) }()

	srv := grpcserver.New(grpcserver.Options{
		Name:       "user",
		Port:       cfg.GRPCPort,
		Log:        log,
		DB:         pool,
		Reflection: cfg.Env == "dev",
		Register: func(s *grpc.Server) {
			userv1.RegisterUserServiceServer(s, user.NewHandler(store, log))
		},
	})
	srvErr := srv.Run(ctx)
	stop()
	wg.Wait()
	return srvErr
}
