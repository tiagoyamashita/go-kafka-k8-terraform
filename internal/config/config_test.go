package config

import "testing"

func TestLoadRequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	if _, err := Load("receiver"); err == nil {
		t.Fatal("expected DATABASE_URL to be required")
	}
}

func TestLoadNormalizesConfluentSettings(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://transfer:transfer@postgres:5432/transfers?sslmode=disable")
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "pkc.example:9092, spare.example:9092")
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "sasl_ssl")
	t.Setenv("KAFKA_SASL_MECHANISM", "scram-sha-256")
	t.Setenv("KAFKA_SASL_USERNAME", "key")
	t.Setenv("KAFKA_SASL_PASSWORD", "secret")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://otel.example:4317")
	t.Setenv("MAX_ATTEMPTS", "3")

	cfg, err := Load("receiver")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Kafka.SecurityProtocol != "SASL_SSL" {
		t.Fatalf("protocol %s", cfg.Kafka.SecurityProtocol)
	}
	if cfg.Kafka.SASLMechanism != "SCRAM-SHA-256" {
		t.Fatalf("mechanism %s", cfg.Kafka.SASLMechanism)
	}
	if cfg.OTELInsecure {
		t.Fatal("https endpoint should use TLS")
	}
	if cfg.OTELEndpoint != "otel.example:4317" {
		t.Fatalf("endpoint %s", cfg.OTELEndpoint)
	}
	if cfg.MaxAttempts != 3 {
		t.Fatalf("attempts %d", cfg.MaxAttempts)
	}
	brokers := cfg.Kafka.Brokers()
	if len(brokers) != 2 || brokers[1] != "spare.example:9092" {
		t.Fatalf("brokers %#v", brokers)
	}
}

func TestLoadRejectsNonPositiveAttempts(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/transfers")
	t.Setenv("MAX_ATTEMPTS", "0")
	if _, err := Load("receiver"); err == nil {
		t.Fatal("expected MAX_ATTEMPTS to be rejected")
	}
}
