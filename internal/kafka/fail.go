package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// Demonstrate forces one produce that this cluster can reject.
// unknown-topic and invalid-topic are sent on the shared client instead.
func Demonstrate(ctx context.Context, brokers []string, mode string) error {
	switch mode {
	case "wrong-address":
		return produceOnce(ctx, []string{"127.0.0.1:1"}, "transfers.in", false)
	case "broker-unreachable":
		return produceOnce(ctx, []string{"192.0.2.1:9092"}, "transfers.in", false)
	case "sasl-rejected":
		if len(brokers) == 0 {
			return fmt.Errorf("no Kafka brokers are configured")
		}
		return produceOnce(ctx, brokers, "transfers.in", true)
	default:
		return fmt.Errorf("unknown kafka failure %s", mode)
	}
}

func produceOnce(ctx context.Context, brokers []string, topic string, sasl bool) error {
	ctx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
	defer cancel()
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RecordRetries(0),
		kgo.UnknownTopicRetries(0),
		kgo.DialTimeout(2 * time.Second),
		kgo.RetryTimeout(2 * time.Second),
	}
	if sasl {
		opts = append(opts, kgo.SASL(plain.Auth{User: "nobody", Pass: "wrong"}.AsMechanism()))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer client.Close()
	err = client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(`{"id":"x"}`)}).FirstErr()
	if err == nil {
		return fmt.Errorf("kafka accepted the record")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("broker unreachable at %s: %w", strings.Join(brokers, ","), err)
	}
	return err
}
