package gateway

import (
	"fmt"

	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
)

type Config struct {
	Env           string
	LogLevel      string
	HTTPPort      int
	AuthAddr      string
	UserAddr      string
	WalletAddr    string
	PaymentAddr   string
	AuthPerMinute int
	UserPerMinute int
}

func LoadConfig() (Config, error) {
	port, err := config.Int("GATEWAY_HTTP_PORT", 8080)
	if err != nil {
		return Config{}, fmt.Errorf("gateway config: %w", err)
	}
	if port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("gateway config: GATEWAY_HTTP_PORT %d is out of range (1-65535)", port)
	}
	authRate, err := config.Int("GATEWAY_AUTH_PER_MINUTE", 20)
	if err != nil {
		return Config{}, fmt.Errorf("gateway config: %w", err)
	}
	userRate, err := config.Int("GATEWAY_USER_PER_MINUTE", 120)
	if err != nil {
		return Config{}, fmt.Errorf("gateway config: %w", err)
	}
	if authRate <= 0 || userRate <= 0 {
		return Config{}, fmt.Errorf("gateway config: rate limits must be positive")
	}
	return Config{
		Env:           config.String("APP_ENV", "dev"),
		LogLevel:      config.String("LOG_LEVEL", "info"),
		HTTPPort:      port,
		AuthAddr:      config.String("AUTH_GRPC_ADDR", "localhost:50055"),
		UserAddr:      config.String("USER_GRPC_ADDR", "localhost:50056"),
		WalletAddr:    config.String("WALLET_GRPC_ADDR", "localhost:50051"),
		PaymentAddr:   config.String("PAYMENT_GRPC_ADDR", "localhost:50053"),
		AuthPerMinute: authRate,
		UserPerMinute: userRate,
	}, nil
}
