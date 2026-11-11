package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the process environment. Kafka fields use the same names as the
// Confluent client so a deployment can point at Confluent Cloud without a code change.
type Config struct {
	ServiceName  string
	DatabaseURL  string
	HTTPAddr     string
	OTELEndpoint string
	OTELInsecure bool
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
	PublishLease time.Duration
	Kafka        Kafka
}

// Kafka holds the consumer and producer connection settings.
type Kafka struct {
	BootstrapServers string
	SecurityProtocol string
	SASLMechanism    string
	SASLUsername     string
	SASLPassword     string
	InboundTopic     string
	OutboundTopic    string
	DLQTopic         string
	GroupID          string
	ClientID         string
}

// Load reads configuration from the environment.
func Load(service string) (Config, error) {
	cfg := Config{
		ServiceName:  env("OTEL_SERVICE_NAME", service),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		HTTPAddr:     env("HTTP_ADDR", ":8080"),
		OTELEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		PollInterval: 2 * time.Second,
		BatchSize:    50,
		MaxAttempts:  5,
		PublishLease: 2 * time.Minute,
		Kafka: Kafka{
			BootstrapServers: env("KAFKA_BOOTSTRAP_SERVERS", "kafka-0.kafka:9092"),
			SecurityProtocol: strings.ToUpper(env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")),
			SASLMechanism:    strings.ToUpper(env("KAFKA_SASL_MECHANISM", "PLAIN")),
			SASLUsername:     os.Getenv("KAFKA_SASL_USERNAME"),
			SASLPassword:     os.Getenv("KAFKA_SASL_PASSWORD"),
			InboundTopic:     env("KAFKA_INBOUND_TOPIC", "transfers.in"),
			OutboundTopic:    env("KAFKA_OUTBOUND_TOPIC", "transfers.out"),
			DLQTopic:         env("KAFKA_DLQ_TOPIC", "transfers.dlq"),
			GroupID:          env("KAFKA_GROUP_ID", "transfer-receiver"),
			ClientID:         env("KAFKA_CLIENT_ID", service),
		},
	}

	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if len(cfg.Kafka.Brokers()) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS is required")
	}

	var err error
	if cfg.PollInterval, err = parseDurationEnv("POLL_INTERVAL", cfg.PollInterval); err != nil {
		return Config{}, err
	}
	if cfg.PublishLease, err = parseDurationEnv("PUBLISH_LEASE", cfg.PublishLease); err != nil {
		return Config{}, err
	}
	if cfg.BatchSize, err = parseIntEnv("BATCH_SIZE", cfg.BatchSize); err != nil {
		return Config{}, err
	}
	if cfg.MaxAttempts, err = parseIntEnv("MAX_ATTEMPTS", cfg.MaxAttempts); err != nil {
		return Config{}, err
	}
	if cfg.PollInterval <= 0 {
		return Config{}, fmt.Errorf("POLL_INTERVAL must be positive")
	}
	if cfg.PublishLease <= 0 {
		return Config{}, fmt.Errorf("PUBLISH_LEASE must be positive")
	}
	if cfg.BatchSize < 1 {
		return Config{}, fmt.Errorf("BATCH_SIZE must be at least 1")
	}
	if cfg.MaxAttempts < 1 {
		return Config{}, fmt.Errorf("MAX_ATTEMPTS must be at least 1")
	}

	cfg.OTELEndpoint, cfg.OTELInsecure = normalizeOTLP(cfg.OTELEndpoint)
	return cfg, nil
}

// Brokers splits the bootstrap server list.
func (k Kafka) Brokers() []string {
	parts := strings.Split(k.BootstrapServers, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func parseDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func parseIntEnv(key string, fallback int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func normalizeOTLP(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	switch {
	case strings.HasPrefix(raw, "https://"):
		return strings.TrimPrefix(raw, "https://"), false
	case strings.HasPrefix(raw, "http://"):
		return strings.TrimPrefix(raw, "http://"), true
	default:
		return raw, true
	}
}
