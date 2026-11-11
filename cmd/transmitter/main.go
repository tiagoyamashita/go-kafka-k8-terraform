package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.opentelemetry.io/otel"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/config"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/httpserver"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/kafka"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/store"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/telemetry"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transmitter"
)

func main() {
	if err := run(); err != nil {
		slog.Error("transmitter stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load("transmitter")
	if err != nil {
		return err
	}
	shutdown, err := telemetry.Setup(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = shutdown(context.Background())
	}()

	pool, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		return err
	}
	pg := store.NewPostgres(pool)

	meter := otel.Meter("github.com/wovenbytoyota/go-kafka-k8-terraform/transmitter")
	metrics, err := telemetry.NewInstruments(meter)
	if err != nil {
		return err
	}
	if err := telemetry.RegisterOutboxDepth(meter, pg.OutboxDepth); err != nil {
		return err
	}

	client, err := kafka.NewClient(cfg.Kafka)
	if err != nil {
		return err
	}
	defer client.Close()

	worker := transmitter.New(pg, kafka.NewProducer(client), transmitter.Options{
		OutboundTopic: cfg.Kafka.OutboundTopic,
		DLQTopic:      cfg.Kafka.DLQTopic,
		BatchSize:     cfg.BatchSize,
		MaxAttempts:   cfg.MaxAttempts,
		Lease:         cfg.PublishLease,
		PollInterval:  cfg.PollInterval,
		Metrics:       metrics,
	})
	ready := func(ctx context.Context) error {
		return pool.Ping(ctx)
	}

	errCh := make(chan error, 2)
	go func() {
		errCh <- httpserver.Run(ctx, cfg.HTTPAddr, ready)
	}()
	go func() {
		errCh <- worker.Run(ctx)
	}()

	err = <-errCh
	stop()
	second := <-errCh
	return firstError(err, second)
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}
