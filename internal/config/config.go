package config

import (
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	RedisAddr         string
	DatabaseURL       string
	HTTPAddr          string
	UIDir             string
	ProcessingTimeout time.Duration
}

func Load() Config {
	_ = godotenv.Load()
	
	return Config{
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		DatabaseURL:       getEnv("DATABASE_URL", "postgres://jobuser:jobpass@localhost:5432/jobscheduler?sslmode=disable"),
		HTTPAddr:          getEnv("HTTP_ADDR", ":8080"),
		UIDir:             getEnv("UI_DIR", "./ui"),
		ProcessingTimeout: getEnvDuration("PROCESSING_TIMEOUT", 60*time.Second),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)

	if v == "" {
		return fallback
	}

	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("invalid %s=%q, using default %s", key, v, fallback)
		return fallback
	}
	return d
}