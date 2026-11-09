package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/config"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/httpserver"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/kafka"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/receiver"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/store"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		slog.Error("receiver stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load("receiver")
	if err != nil {
		return err
	}
	if cfg.Kafka.GroupID == "" {
		return errors.New("KAFKA_GROUP_ID is required")
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

	metrics, err := telemetry.NewInstruments(otel.Meter("github.com/wovenbytoyota/go-kafka-k8-terraform/receiver"))
	if err != nil {
		return err
	}

	client, err := kafka.NewClient(cfg.Kafka,
		kgo.ConsumerGroup(cfg.Kafka.GroupID),
		kgo.ConsumeTopics(cfg.Kafka.InboundTopic),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	svc := receiver.New(store.NewPostgres(pool), kafka.NewProducer(client), cfg.Kafka.DLQTopic, metrics)
	ready := func(ctx context.Context) error {
		if err := pool.Ping(ctx); err != nil {
			return err
		}
		return client.Ping(ctx)
	}

	errCh := make(chan error, 2)
	go func() {
		errCh <- httpserver.Run(ctx, cfg.HTTPAddr, ready)
	}()
	go func() {
		errCh <- kafka.Consume(ctx, client, svc)
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
