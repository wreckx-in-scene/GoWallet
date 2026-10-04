package fraud

import (
	"fmt"

	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
)

// Config holds every setting the fraud service needs.
type Config struct {
	Env            string
	LogLevel       string
	GRPCPort       int
	DBURL          string
	MaxAmountPaise int64
	MaxPerMinute   int
}

func LoadConfig() (Config, error) {
	dbURL, err := config.Required("FRAUD_DB_URL")
	if err != nil {
		return Config{}, fmt.Errorf("fraud config: %w", err)
	}

	port, err := config.Int("FRAUD_GRPC_PORT", 50052)
	if err != nil {
		return Config{}, fmt.Errorf("fraud config: %w", err)
	}
	if port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("fraud config: FRAUD_GRPC_PORT %d is out of range (1-65535)", port)
	}

	maxAmount, err := config.Int("FRAUD_MAX_AMOUNT_PAISE", 5_000_000) // Rs 50,000
	if err != nil {
		return Config{}, fmt.Errorf("fraud config: %w", err)
	}
	perMinute, err := config.Int("FRAUD_MAX_PER_MINUTE", 5)
	if err != nil {
		return Config{}, fmt.Errorf("fraud config: %w", err)
	}
	if maxAmount <= 0 || perMinute <= 0 {
		return Config{}, fmt.Errorf("fraud config: FRAUD_MAX_AMOUNT_PAISE and FRAUD_MAX_PER_MINUTE must be positive")
	}

	return Config{
		Env:            config.String("APP_ENV", "dev"),
		LogLevel:       config.String("LOG_LEVEL", "info"),
		GRPCPort:       port,
		DBURL:          dbURL,
		MaxAmountPaise: int64(maxAmount),
		MaxPerMinute:   perMinute,
	}, nil
}
