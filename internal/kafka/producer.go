package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes one record and waits for the broker ack.
type Producer interface {
	Produce(ctx context.Context, topic, key string, value []byte) error
}

// FranzProducer publishes with the shared franz-go client.
type FranzProducer struct {
	client *kgo.Client
}

// NewProducer wraps a client.
func NewProducer(client *kgo.Client) *FranzProducer {
	return &FranzProducer{client: client}
}

// Produce sends value as the record body. The key is the transfer id for outbound messages.
func (p *FranzProducer) Produce(ctx context.Context, topic, key string, value []byte) error {
	headers := make([]kgo.RecordHeader, 0)
	injectTrace(ctx, &headers)
	record := &kgo.Record{
		Topic:   topic,
		Key:     append([]byte(nil), key...),
		Value:   append([]byte(nil), value...),
		Headers: headers,
		Context: ctx,
	}
	return p.client.ProduceSync(ctx, record).FirstErr()
}
