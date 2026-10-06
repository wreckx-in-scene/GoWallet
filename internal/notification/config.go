package notification

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
	FailPercent  int // dev only: simulate provider failures
}

func LoadConfig() (Config, error) {
	dbURL, err := config.Required("NOTIFICATION_DB_URL")
	if err != nil {
		return Config{}, fmt.Errorf("notification config: %w", err)
	}
	brokers, err := config.Required("KAFKA_BROKERS")
	if err != nil {
		return Config{}, fmt.Errorf("notification config: %w", err)
	}
	port, err := config.Int("NOTIFICATION_GRPC_PORT", 50057)
	if err != nil {
		return Config{}, fmt.Errorf("notification config: %w", err)
	}
	if port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("notification config: NOTIFICATION_GRPC_PORT %d is out of range (1-65535)", port)
	}
	fail, err := config.Int("NOTIFY_FAIL_PERCENT", 0)
	if err != nil {
		return Config{}, fmt.Errorf("notification config: %w", err)
	}
	if fail < 0 || fail > 100 {
		return Config{}, fmt.Errorf("notification config: NOTIFY_FAIL_PERCENT must be 0-100")
	}
	return Config{
		Env:          config.String("APP_ENV", "dev"),
		LogLevel:     config.String("LOG_LEVEL", "info"),
		GRPCPort:     port,
		DBURL:        dbURL,
		KafkaBrokers: config.SplitList(brokers),
		FailPercent:  fail,
	}, nil
}
