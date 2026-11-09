package kafka

import "testing"

import "github.com/wovenbytoyota/go-kafka-k8-terraform/internal/config"

func TestDialSettings(t *testing.T) {
	mech, useTLS, err := DialSettings(config.Kafka{SecurityProtocol: "PLAINTEXT"})
	if err != nil || mech != nil || useTLS {
		t.Fatalf("plaintext mech %v tls %v err %v", mech, useTLS, err)
	}

	mech, useTLS, err = DialSettings(config.Kafka{
		SecurityProtocol: "SASL_SSL",
		SASLMechanism:    "PLAIN",
		SASLUsername:     "key",
		SASLPassword:     "secret",
	})
	if err != nil || !useTLS || mech == nil || mech.Name() != "PLAIN" {
		t.Fatalf("sasl ssl mech %v tls %v err %v", mech, useTLS, err)
	}

	mech, useTLS, err = DialSettings(config.Kafka{
		SecurityProtocol: "SASL_PLAINTEXT",
		SASLMechanism:    "SCRAM-SHA-512",
		SASLUsername:     "key",
		SASLPassword:     "secret",
	})
	if err != nil || useTLS || mech == nil || mech.Name() != "SCRAM-SHA-512" {
		t.Fatalf("scram mech %v tls %v err %v", mech, useTLS, err)
	}

	if _, _, err := DialSettings(config.Kafka{SecurityProtocol: "SASL_SSL"}); err == nil {
		t.Fatal("expected missing credentials to fail")
	}
	if _, _, err := DialSettings(config.Kafka{SecurityProtocol: "MTLS"}); err == nil {
		t.Fatal("expected unknown protocol to fail")
	}
}
