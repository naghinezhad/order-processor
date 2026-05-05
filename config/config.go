package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	ServerPort            string
	PostgresDSN           string
	RedisAddr             string
	RedisPass             string
	KafkaBrokers          []string
	KafkaTopic            string
	KafkaDLTTopic         string
	ConsumerGroupID       string
	ConsumerID            string
	LockTTLSeconds        int
	RequestTimeoutSeconds int
}

func LoadConfig() *Config {
	serverPort := getEnv("SERVER_PORT", "8080")
	postgresDSN := getEnv("POSTGRES_DSN", "postgres://order:order@localhost:5432/orderdb?sslmode=disable")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPass := os.Getenv("REDIS_PASS")
	kafkaBrokers := splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092"))
	kafkaTopic := getEnv("KAFKA_TOPIC", "order-requested")
	kafkaDLTTopic := getEnv("KAFKA_DLT_TOPIC", "order-requested-dlt")
	consumerGroupID := getEnv("CONSUMER_GROUP_ID", "order-processor")
	consumerID := getEnv("CONSUMER_ID", "consumer-1")
	lockTTLSeconds := getEnvInt("LOCK_TTL_SECONDS", 30)
	requestTimeoutSeconds := getEnvInt("REQUEST_TIMEOUT_SECONDS", 5)

	return &Config{
		ServerPort:            serverPort,
		PostgresDSN:           postgresDSN,
		RedisAddr:             redisAddr,
		RedisPass:             redisPass,
		KafkaBrokers:          kafkaBrokers,
		KafkaTopic:            kafkaTopic,
		KafkaDLTTopic:         kafkaDLTTopic,
		ConsumerGroupID:       consumerGroupID,
		ConsumerID:            consumerID,
		LockTTLSeconds:        lockTTLSeconds,
		RequestTimeoutSeconds: requestTimeoutSeconds,
	}
}

func getEnv(key string, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}

	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}

	parsed, err := strconv.Atoi(val)
	if err != nil {
		return defaultValue
	}

	return parsed
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
