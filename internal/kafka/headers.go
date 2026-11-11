package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
)

type recordCarrier struct {
	headers *[]kgo.RecordHeader
}

func (c recordCarrier) Get(key string) string {
	for _, header := range *c.headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func (c recordCarrier) Set(key, value string) {
	for i, header := range *c.headers {
		if header.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c recordCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, header := range *c.headers {
		keys = append(keys, header.Key)
	}
	return keys
}

// ContextFromRecord extracts a W3C trace context from Kafka headers.
func ContextFromRecord(ctx context.Context, headers []kgo.RecordHeader) context.Context {
	copied := append([]kgo.RecordHeader(nil), headers...)
	return otel.GetTextMapPropagator().Extract(ctx, recordCarrier{headers: &copied})
}

func injectTrace(ctx context.Context, headers *[]kgo.RecordHeader) {
	otel.GetTextMapPropagator().Inject(ctx, recordCarrier{headers: headers})
}
