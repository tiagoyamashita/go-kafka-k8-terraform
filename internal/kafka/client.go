package kafka

import (
	"crypto/tls"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/config"
)

// DialSettings maps Confluent security.protocol and sasl.mechanism values onto franz-go.
func DialSettings(cfg config.Kafka) (sasl.Mechanism, bool, error) {
	switch cfg.SecurityProtocol {
	case "PLAINTEXT", "":
		return nil, false, nil
	case "SSL":
		return nil, true, nil
	case "SASL_PLAINTEXT":
		mech, err := saslMechanism(cfg)
		return mech, false, err
	case "SASL_SSL":
		mech, err := saslMechanism(cfg)
		return mech, true, err
	default:
		return nil, false, fmt.Errorf("unsupported security protocol %q", cfg.SecurityProtocol)
	}
}

func saslMechanism(cfg config.Kafka) (sasl.Mechanism, error) {
	if cfg.SASLUsername == "" || cfg.SASLPassword == "" {
		return nil, fmt.Errorf("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required for %s", cfg.SecurityProtocol)
	}
	switch cfg.SASLMechanism {
	case "PLAIN", "":
		return plain.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q", cfg.SASLMechanism)
	}
}

// NewClient builds a Kafka client. extra opts add the consumer group or producer settings.
func NewClient(cfg config.Kafka, extra ...kgo.Opt) (*kgo.Client, error) {
	mech, useTLS, err := DialSettings(cfg)
	if err != nil {
		return nil, err
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers()...),
		kgo.ClientID(cfg.ClientID),
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}
	if useTLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	opts = append(opts, extra...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return client, nil
}
