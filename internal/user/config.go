package user

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
	dbURL, err := config.Required("USER_DB_URL")
	if err != nil {
		return Config{}, fmt.Errorf("user config: %w", err)
	}
	brokers, err := config.Required("KAFKA_BROKERS")
	if err != nil {
		return Config{}, fmt.Errorf("user config: %w", err)
	}
	port, err := config.Int("USER_GRPC_PORT", 50056)
	if err != nil {
		return Config{}, fmt.Errorf("user config: %w", err)
	}
	if port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("user config: USER_GRPC_PORT %d is out of range (1-65535)", port)
	}
	return Config{
		Env:          config.String("APP_ENV", "dev"),
		LogLevel:     config.String("LOG_LEVEL", "info"),
		GRPCPort:     port,
		DBURL:        dbURL,
		KafkaBrokers: config.SplitList(brokers),
	}, nil
}
