package wallet

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
}

func LoadConfig() (Config, error) {
	dbURL, err := config.Required("WALLET_DB_URL")
	if err != nil {
		return Config{}, fmt.Errorf("wallet config: %w", err)
	}

	brokers, err := config.Required("KAFKA_BROKERS")
	if err != nil {
		return Config{}, fmt.Errorf("wallet config: %w", err)
	}
	port, err := config.Int("WALLET_GRPC_PORT", 50051)
	if err != nil {
		return Config{}, fmt.Errorf("wallet config: %w", err)
	}

	return Config{
		Env:          config.String("APP_ENV", "dev"),
		LogLevel:     config.String("LOG_LEVEL", "info"),
		GRPCPort:     port,
		DBURL:        dbURL,
		KafkaBrokers: config.SplitList(brokers),
	}, nil
}
