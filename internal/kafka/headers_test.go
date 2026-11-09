package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceContextRoundTrip(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})
	ctx, span := provider.Tracer("test").Start(context.Background(), "produce")
	defer span.End()

	headers := make([]kgo.RecordHeader, 0)
	injectTrace(ctx, &headers)
	extracted := ContextFromRecord(context.Background(), headers)
	if trace.SpanContextFromContext(extracted).TraceID() != span.SpanContext().TraceID() {
		t.Fatal("trace id was not propagated")
	}
}
