package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// go doesnt read en files
// loaddotenv reads the .env key values pairs and put them into process env
func LoadDotEnv() error {
	err := godotenv.Load()
	if err != nil {
		return fmt.Errorf("loading .env: %w", err)
	}

	return nil
}

// gives the required env variable , fail if not set
func Required(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("Required environment variable %s is not set", key)
	}

	return v, nil
}

func String(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

func Int(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("environment variable %s must be an integer, got %q: %w", key, v, err)
	}

	return n, nil
}

// SplitList splits a comma-separated value like "a:9092,b:9092", dropping blanks.
func SplitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
