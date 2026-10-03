package main

import (
	"fmt"
	"os"

	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/logger"
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
	return nil
}
