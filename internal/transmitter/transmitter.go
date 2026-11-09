package transmitter

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/kafka"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/telemetry"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

// Store is the outbox the transmitter claims from.
type Store interface {
	ClaimPublish(ctx context.Context, limit int, lease time.Duration, maxAttempts int) ([]transfer.Record, error)
	ClaimDead(ctx context.Context, limit int, lease time.Duration, maxAttempts int) ([]transfer.Record, error)
	MarkPublished(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id, message string) error
	MarkDead(ctx context.Context, id, message string) error
	OutboxDepth(ctx context.Context) (int64, error)
}

// Options controls polling and the outbound topics.
type Options struct {
	OutboundTopic string
	DLQTopic      string
	BatchSize     int
	MaxAttempts   int
	Lease         time.Duration
	PollInterval  time.Duration
	Metrics       *telemetry.Instruments
}

// Worker publishes claimed JSON payloads and dead-letters rows that exhaust their attempts.
type Worker struct {
	store    Store
	producer kafka.Producer
	opt      Options
}

// New builds a transmitter worker.
func New(store Store, producer kafka.Producer, opt Options) *Worker {
	return &Worker{store: store, producer: producer, opt: opt}
}

// Run polls until the context is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	for {
		n, err := w.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "transmit batch", "err", err)
			w.opt.Metrics.AddErrors(ctx, 1)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if n > 0 {
			continue
		}
		timer := time.NewTimer(w.opt.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// RunOnce claims one batch, publishes it, and dead-letters exhausted rows.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "transfer.claim")
	defer span.End()

	publishing, err := w.store.ClaimPublish(ctx, w.opt.BatchSize, w.opt.Lease, w.opt.MaxAttempts)
	if err != nil {
		span.RecordError(err)
		return 0, err
	}
	dead, err := w.store.ClaimDead(ctx, w.opt.BatchSize, w.opt.Lease, w.opt.MaxAttempts)
	if err != nil {
		span.RecordError(err)
		return 0, err
	}

	var processed int
	var first error
	for _, rec := range publishing {
		if err := w.publish(ctx, rec); err != nil && first == nil {
			first = err
		}
		processed++
	}
	for _, rec := range dead {
		if err := w.deadLetter(ctx, rec, rec.LastError); err != nil && first == nil {
			first = err
		}
		processed++
	}
	return processed, first
}

func (w *Worker) publish(ctx context.Context, rec transfer.Record) error {
	ctx, span := telemetry.Tracer().Start(ctx, "transfer.publish", trace.WithAttributes(
		attribute.String("transfer.id", rec.ID),
		attribute.String("messaging.destination.name", w.opt.OutboundTopic),
	))
	defer span.End()

	start := time.Now()
	err := w.producer.Produce(ctx, w.opt.OutboundTopic, rec.ID, rec.Payload)
	w.opt.Metrics.RecordPublish(ctx, time.Since(start))
	if err == nil {
		w.opt.Metrics.AddPublished(ctx, 1)
		return w.store.MarkPublished(ctx, rec.ID)
	}
	span.RecordError(err)
	if rec.Attempts >= w.opt.MaxAttempts {
		return w.deadLetter(ctx, rec, err.Error())
	}
	w.opt.Metrics.AddRetries(ctx, 1)
	return w.store.MarkFailed(ctx, rec.ID, err.Error())
}

func (w *Worker) deadLetter(ctx context.Context, rec transfer.Record, reason string) error {
	ctx, span := telemetry.Tracer().Start(ctx, "transfer.deadletter", trace.WithAttributes(
		attribute.String("transfer.id", rec.ID),
	))
	defer span.End()

	if err := w.producer.Produce(ctx, w.opt.DLQTopic, rec.ID, rec.Payload); err != nil {
		span.RecordError(err)
		if markErr := w.store.MarkFailed(ctx, rec.ID, err.Error()); markErr != nil {
			return markErr
		}
		return err
	}
	w.opt.Metrics.AddDLQ(ctx, 1)
	return w.store.MarkDead(ctx, rec.ID, reason)
}
