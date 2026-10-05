package auth

import (
	"fmt"
	"time"

	"github.com/wreckx-in-scene/GoWallet/internal/platform/config"
)

type Config struct {
	Env          string
	LogLevel     string
	GRPCPort     int
	DBURL        string
	KafkaBrokers []string
	KeyFile      string
	AccessTTL    time.Duration
	RefreshTTL   time.Duration
}

func LoadConfig() (Config, error) {
	dbURL, err := config.Required("AUTH_DB_URL")
	if err != nil {
		return Config{}, fmt.Errorf("auth config: %w", err)
	}
	brokers, err := config.Required("KAFKA_BROKERS")
	if err != nil {
		return Config{}, fmt.Errorf("auth config: %w", err)
	}
	port, err := config.Int("AUTH_GRPC_PORT", 50055)
	if err != nil {
		return Config{}, fmt.Errorf("auth config: %w", err)
	}
	if port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("auth config: AUTH_GRPC_PORT %d is out of range (1-65535)", port)
	}
	accessSecs, err := config.Int("AUTH_ACCESS_TTL_SECONDS", 900)
	if err != nil {
		return Config{}, fmt.Errorf("auth config: %w", err)
	}
	refreshHours, err := config.Int("AUTH_REFRESH_TTL_HOURS", 720)
	if err != nil {
		return Config{}, fmt.Errorf("auth config: %w", err)
	}
	if accessSecs <= 0 || refreshHours <= 0 {
		return Config{}, fmt.Errorf("auth config: token lifetimes must be positive")
	}
	return Config{
		Env:          config.String("APP_ENV", "dev"),
		LogLevel:     config.String("LOG_LEVEL", "info"),
		GRPCPort:     port,
		DBURL:        dbURL,
		KafkaBrokers: config.SplitList(brokers),
		KeyFile:      config.String("AUTH_KEY_FILE", "./keys/auth-ed25519.pem"),
		AccessTTL:    time.Duration(accessSecs) * time.Second,
		RefreshTTL:   time.Duration(refreshHours) * time.Hour,
	}, nil
}
