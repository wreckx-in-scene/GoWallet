package payment

import (
	"fmt"

	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
)

type Config struct {
	Env          string
	LogLevel     string
	GRPCPort     int
	DBURL        string
	KafkaBrokers []string
	WalletAddr   string
	FraudAddr    string
}

func LoadConfig() (Config, error) {
	dbURL, err := config.Required("PAYMENT_DB_URL")
	if err != nil {
		return Config{}, fmt.Errorf("payment config: %w", err)
	}
	brokers, err := config.Required("KAFKA_BROKERS")
	if err != nil {
		return Config{}, fmt.Errorf("payment config: %w", err)
	}
	port, err := config.Int("PAYMENT_GRPC_PORT", 50053)
	if err != nil {
		return Config{}, fmt.Errorf("payment config: %w", err)
	}
	if port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("payment config: PAYMENT_GRPC_PORT %d is out of range (1-65535)", port)
	}
	return Config{
		Env:          config.String("APP_ENV", "dev"),
		LogLevel:     config.String("LOG_LEVEL", "info"),
		GRPCPort:     port,
		DBURL:        dbURL,
		KafkaBrokers: config.SplitList(brokers),
		WalletAddr:   config.String("WALLET_GRPC_ADDR", "localhost:50051"),
		FraudAddr:    config.String("FRAUD_GRPC_ADDR", "localhost:50052"),
	}, nil
}
