package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	authv1 "github.com/wreckx-in-scene/GoWallet/gen/auth/v1"
	paymentv1 "github.com/wreckx-in-scene/GoWallet/gen/payment/v1"
	userv1 "github.com/wreckx-in-scene/GoWallet/gen/user/v1"
	walletv1 "github.com/wreckx-in-scene/GoWallet/gen/wallet/v1"
	"github.com/wreckx-in-scene/GoWallet/internal/gateway"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/logger"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gateway:", err)
		os.Exit(1)
	}
}

func run() error {
	if err := config.LoadDotEnv(); err != nil {
		return err
	}
	cfg, err := gateway.LoadConfig()
	if err != nil {
		return err
	}
	log, err := logger.New(cfg.Env, cfg.LogLevel)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := metrics.Serve(ctx, log, "gateway"); err != nil {
		return err
	}

	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	var conns []*grpc.ClientConn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	dial := func(addr string) (*grpc.ClientConn, error) {
		c, err := grpc.NewClient(addr, creds)
		if err == nil {
			conns = append(conns, c)
		}
		return c, err
	}

	authConn, err := dial(cfg.AuthAddr)
	if err != nil {
		return fmt.Errorf("auth client: %w", err)
	}
	userConn, err := dial(cfg.UserAddr)
	if err != nil {
		return fmt.Errorf("user client: %w", err)
	}
	walletConn, err := dial(cfg.WalletAddr)
	if err != nil {
		return fmt.Errorf("wallet client: %w", err)
	}
	paymentConn, err := dial(cfg.PaymentAddr)
	if err != nil {
		return fmt.Errorf("payment client: %w", err)
	}

	srv := gateway.NewServer(cfg, log,
		authv1.NewAuthServiceClient(authConn),
		userv1.NewUserServiceClient(userConn),
		walletv1.NewWalletServiceClient(walletConn),
		paymentv1.NewPaymentServiceClient(paymentConn),
	)
	return srv.Run(ctx)
}
