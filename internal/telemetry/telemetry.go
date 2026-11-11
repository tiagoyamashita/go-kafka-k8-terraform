package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/config"
)

const tracerName = "github.com/wovenbytoyota/go-kafka-k8-terraform/transfer"

// Setup installs the global tracer, meter, and JSON logger.
// Call NewInstruments only after Setup so instruments bind to this provider.
func Setup(ctx context.Context, cfg config.Config) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", cfg.ServiceName),
	))
	if err != nil {
		return nil, err
	}

	tracerOpts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	var shutdownTrace func(context.Context) error
	if cfg.OTELEndpoint != "" {
		traceOpts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.OTELEndpoint)}
		if cfg.OTELInsecure {
			traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
		}
		exp, err := otlptracegrpc.New(ctx, traceOpts...)
		if err != nil {
			return nil, err
		}
		tracerOpts = append(tracerOpts, sdktrace.WithBatcher(exp))
		shutdownTrace = exp.Shutdown
	}
	tp := sdktrace.NewTracerProvider(tracerOpts...)
	otel.SetTracerProvider(tp)

	promExporter, err := otelprom.New()
	if err != nil {
		return nil, err
	}
	meterOpts := []sdkmetric.Option{
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promExporter),
	}
	var shutdownMetricExport func(context.Context) error
	if cfg.OTELEndpoint != "" {
		metricOpts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(cfg.OTELEndpoint)}
		if cfg.OTELInsecure {
			metricOpts = append(metricOpts, otlpmetricgrpc.WithInsecure())
		}
		exp, err := otlpmetricgrpc.New(ctx, metricOpts...)
		if err != nil {
			return nil, err
		}
		meterOpts = append(meterOpts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
		shutdownMetricExport = exp.Shutdown
	}
	mp := sdkmetric.NewMeterProvider(meterOpts...)
	otel.SetMeterProvider(mp)

	slog.SetDefault(slog.New(contextHandler{
		Handler: slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
	}))

	shutdown := func(ctx context.Context) error {
		var errs []error
		if err := mp.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
		if shutdownMetricExport != nil {
			if err := shutdownMetricExport(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if err := tp.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
		if shutdownTrace != nil {
			if err := shutdownTrace(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	return shutdown, nil
}

// Tracer returns the process tracer.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// Instruments are the transfer counters and histograms scraped from /metrics.
type Instruments struct {
	received        metric.Int64Counter
	duplicates      metric.Int64Counter
	published       metric.Int64Counter
	retries         metric.Int64Counter
	dlq             metric.Int64Counter
	errors          metric.Int64Counter
	persistDuration metric.Float64Histogram
	publishDuration metric.Float64Histogram
}

// NewInstruments registers transfer metrics on meter.
func NewInstruments(m metric.Meter) (*Instruments, error) {
	received, err := m.Int64Counter("transfer.received", metric.WithDescription("Inbound messages stored or recognized as duplicates"))
	if err != nil {
		return nil, err
	}
	duplicates, err := m.Int64Counter("transfer.duplicates", metric.WithDescription("Inbound messages already stored"))
	if err != nil {
		return nil, err
	}
	published, err := m.Int64Counter("transfer.published", metric.WithDescription("Transfers published to the outbound topic"))
	if err != nil {
		return nil, err
	}
	retries, err := m.Int64Counter("transfer.retries", metric.WithDescription("Publish attempts that will be retried"))
	if err != nil {
		return nil, err
	}
	dlq, err := m.Int64Counter("transfer.dlq", metric.WithDescription("Messages written to the dead-letter topic"))
	if err != nil {
		return nil, err
	}
	errs, err := m.Int64Counter("transfer.errors", metric.WithDescription("Handler and publish errors"))
	if err != nil {
		return nil, err
	}
	persistDuration, err := m.Float64Histogram("transfer.persist.duration", metric.WithUnit("s"), metric.WithDescription("Time to insert a transfer"))
	if err != nil {
		return nil, err
	}
	publishDuration, err := m.Float64Histogram("transfer.publish.duration", metric.WithUnit("s"), metric.WithDescription("Time to publish a transfer"))
	if err != nil {
		return nil, err
	}
	return &Instruments{
		received:        received,
		duplicates:      duplicates,
		published:       published,
		retries:         retries,
		dlq:             dlq,
		errors:          errs,
		persistDuration: persistDuration,
		publishDuration: publishDuration,
	}, nil
}

// RegisterOutboxDepth observes how many transfers are still unpublished.
func RegisterOutboxDepth(m metric.Meter, depth func(context.Context) (int64, error)) error {
	gauge, err := m.Int64ObservableGauge("transfer.outbox.depth", metric.WithDescription("Transfers waiting to be published"))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		n, err := depth(ctx)
		if err != nil {
			return nil
		}
		observer.ObserveInt64(gauge, n)
		return nil
	}, gauge)
	return err
}

// AddReceived increments the received counter.
func (m *Instruments) AddReceived(ctx context.Context, n int64) {
	if m == nil {
		return
	}
	m.received.Add(ctx, n)
}

// AddDuplicates increments the duplicate counter.
func (m *Instruments) AddDuplicates(ctx context.Context, n int64) {
	if m == nil {
		return
	}
	m.duplicates.Add(ctx, n)
}

// AddPublished increments the published counter.
func (m *Instruments) AddPublished(ctx context.Context, n int64) {
	if m == nil {
		return
	}
	m.published.Add(ctx, n)
}

// AddRetries increments the retry counter.
func (m *Instruments) AddRetries(ctx context.Context, n int64) {
	if m == nil {
		return
	}
	m.retries.Add(ctx, n)
}

// AddDLQ increments the dead-letter counter.
func (m *Instruments) AddDLQ(ctx context.Context, n int64) {
	if m == nil {
		return
	}
	m.dlq.Add(ctx, n)
}

// AddErrors increments the error counter.
func (m *Instruments) AddErrors(ctx context.Context, n int64) {
	if m == nil {
		return
	}
	m.errors.Add(ctx, n)
}

// RecordPersist records an insert duration.
func (m *Instruments) RecordPersist(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.persistDuration.Record(ctx, d.Seconds())
}

// RecordPublish records a produce duration.
func (m *Instruments) RecordPublish(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.publishDuration.Record(ctx, d.Seconds())
}

type contextHandler struct {
	slog.Handler
}

func (h contextHandler) Handle(ctx context.Context, record slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, record)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{Handler: h.Handler.WithGroup(name)}
}
