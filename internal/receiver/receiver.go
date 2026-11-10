package receiver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/kafka"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/telemetry"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

// Store persists a received transfer. Inserted is false when the id or source offset already exists.
type Store interface {
	InsertReceived(ctx context.Context, rec transfer.Record) (bool, error)
}

// Service consumes inbound JSON and writes it to Postgres.
type Service struct {
	store    Store
	dlq      kafka.Producer
	dlqTopic string
	metrics  *telemetry.Instruments
}

// New builds a receiver. metrics may be nil in tests.
func New(store Store, dlq kafka.Producer, dlqTopic string, metrics *telemetry.Instruments) *Service {
	return &Service{store: store, dlq: dlq, dlqTopic: dlqTopic, metrics: metrics}
}

// Handle stores a valid JSON message. Invalid messages are published to the DLQ and not stored.
// A DLQ publish failure is returned so the source offset stays uncommitted.
func (s *Service) Handle(ctx context.Context, msg transfer.Inbound) error {
	ctx, span := telemetry.Tracer().Start(ctx, "transfer.consume", trace.WithAttributes(
		attribute.String("messaging.destination.name", msg.Topic),
		attribute.Int64("messaging.kafka.offset", msg.Offset),
	))
	defer span.End()

	if len(msg.Value) > transfer.MaxBodyBytes {
		err := fmt.Errorf("message is larger than 64KiB")
		span.RecordError(err)
		return s.toDLQ(ctx, msg, err)
	}
	rec, err := transfer.Parse(msg.Value)
	if err != nil {
		span.RecordError(err)
		return s.toDLQ(ctx, msg, err)
	}
	rec.MessageKey = string(msg.Key)
	if rec.MessageKey == "" {
		rec.MessageKey = rec.ID
	}
	rec.SourceTopic = msg.Topic
	rec.Partition = msg.Partition
	rec.Offset = msg.Offset
	span.SetAttributes(attribute.String("transfer.id", rec.ID))

	insertCtx, insertSpan := telemetry.Tracer().Start(ctx, "transfer.insert")
	start := time.Now()
	inserted, err := s.store.InsertReceived(insertCtx, rec)
	s.metrics.RecordPersist(insertCtx, time.Since(start))
	if err != nil {
		insertSpan.RecordError(err)
		insertSpan.End()
		s.metrics.AddErrors(insertCtx, 1)
		return err
	}
	insertSpan.End()

	s.metrics.AddReceived(ctx, 1)
	if !inserted {
		s.metrics.AddDuplicates(ctx, 1)
	}
	return nil
}

func (s *Service) toDLQ(ctx context.Context, msg transfer.Inbound, reason error) error {
	ctx, span := telemetry.Tracer().Start(ctx, "transfer.deadletter")
	defer span.End()
	key := string(msg.Key)
	if key == "" {
		key = "invalid"
	}
	if err := s.dlq.Produce(ctx, s.dlqTopic, key, deadLetterValue(reason.Error(), msg.Value)); err != nil {
		span.RecordError(err)
		s.metrics.AddErrors(ctx, 1)
		return err
	}
	s.metrics.AddDLQ(ctx, 1)
	return nil
}

func deadLetterValue(reason string, raw []byte) []byte {
	body := map[string]any{"error": reason}
	if json.Valid(raw) {
		copied := append([]byte(nil), raw...)
		body["message"] = json.RawMessage(copied)
	} else if len(raw) > 0 {
		body["raw"] = string(raw)
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return []byte(`{"error":"invalid transfer message"}`)
	}
	return encoded
}
