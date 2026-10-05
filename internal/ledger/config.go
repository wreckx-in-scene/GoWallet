package ledger

import (
	"fmt"
	"time"

	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
)

type Config struct {
	Env            string
	LogLevel       string
	GRPCPort       int
	DBURL          string
	KafkaBrokers   []string
	WalletAddr     string
	ReconcileEvery time.Duration
}

func LoadConfig() (Config, error) {
	dbURL, err := config.Required("LEDGER_DB_URL")
	if err != nil {
		return Config{}, fmt.Errorf("ledger config: %w", err)
	}
	brokers, err := config.Required("KAFKA_BROKERS")
	if err != nil {
		return Config{}, fmt.Errorf("ledger config: %w", err)
	}
	port, err := config.Int("LEDGER_GRPC_PORT", 50054)
	if err != nil {
		return Config{}, fmt.Errorf("ledger config: %w", err)
	}
	if port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("ledger config: LEDGER_GRPC_PORT %d is out of range (1-65535)", port)
	}
	secs, err := config.Int("LEDGER_RECONCILE_SECONDS", 60)
	if err != nil {
		return Config{}, fmt.Errorf("ledger config: %w", err)
	}
	if secs <= 0 {
		return Config{}, fmt.Errorf("ledger config: LEDGER_RECONCILE_SECONDS must be positive")
	}
	return Config{
		Env:            config.String("APP_ENV", "dev"),
		LogLevel:       config.String("LOG_LEVEL", "info"),
		GRPCPort:       port,
		DBURL:          dbURL,
		KafkaBrokers:   config.SplitList(brokers),
		WalletAddr:     config.String("WALLET_GRPC_ADDR", "localhost:50051"),
		ReconcileEvery: time.Duration(secs) * time.Second,
	}, nil
}
