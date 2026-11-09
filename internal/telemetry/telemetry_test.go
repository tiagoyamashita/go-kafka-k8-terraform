package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
)

func TestPrometheusNames(t *testing.T) {
	reg := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		t.Fatal(err)
	}
	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	meter := provider.Meter("test")
	instruments, err := NewInstruments(meter)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	instruments.AddReceived(ctx, 1)
	instruments.AddPublished(ctx, 1)
	instruments.AddRetries(ctx, 1)
	instruments.AddDLQ(ctx, 1)
	instruments.AddErrors(ctx, 1)
	instruments.RecordPersist(ctx, 50*time.Millisecond)
	instruments.RecordPublish(ctx, 20*time.Millisecond)
	if err := RegisterOutboxDepth(meter, func(context.Context) (int64, error) { return 4, nil }); err != nil {
		t.Fatal(err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, family := range families {
		names[family.GetName()] = true
	}
	for _, name := range []string{
		"transfer_received_total",
		"transfer_published_total",
		"transfer_retries_total",
		"transfer_dlq_total",
		"transfer_errors_total",
		"transfer_persist_duration_seconds",
		"transfer_publish_duration_seconds",
		"transfer_outbox_depth",
	} {
		if !names[name] {
			t.Fatalf("missing %s in %#v", name, names)
		}
	}
}
