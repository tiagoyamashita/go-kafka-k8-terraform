package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/config"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/console"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/kafka"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("console stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load("console")
	if err != nil {
		return err
	}
	pool, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		return err
	}

	client, err := kafka.NewClient(cfg.Kafka)
	if err != nil {
		return err
	}
	defer client.Close()

	letters := console.NewDeadLetters()
	ui, err := console.New(kafka.NewProducer(client), store.NewPostgres(pool), cfg.Kafka.InboundTopic, letters)
	if err != nil {
		return err
	}
	ui.SetBrokers(cfg.Kafka.Brokers())
	// A new group on each start reads the topic from the beginning into the pane.
	// The receiver group must stay untouched.
	dlqClient, err := kafka.NewClient(cfg.Kafka,
		kgo.ConsumerGroup("transfer-console-"+uuid.NewString()),
		kgo.ConsumeTopics(cfg.Kafka.DLQTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
	)
	if err != nil {
		return err
	}
	defer dlqClient.Close()
	go func() {
		err := kafka.Consume(ctx, dlqClient, letters)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("dead-letter watch stopped", "err", err)
		}
	}()

	mux := http.NewServeMux()
	ui.Register(mux)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		if err := client.Ping(r.Context()); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		if err := dlqClient.Ping(r.Context()); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return <-errCh
	case err := <-errCh:
		return err
	}
}
