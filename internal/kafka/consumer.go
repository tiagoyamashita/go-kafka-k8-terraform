package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

// Handler processes one inbound record. A nil error means the offset may be committed.
type Handler interface {
	Handle(ctx context.Context, msg transfer.Inbound) error
}

// Consume polls the group, commits only records the handler accepted, and retries the rest.
// Delivery is at-least-once: a crash after a side effect and before the commit redelivers.
func Consume(ctx context.Context, client *kgo.Client, handler Handler) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}

		fetchErrs := fetches.Errors()
		for _, fetchErr := range fetchErrs {
			slog.ErrorContext(ctx, "kafka fetch", "topic", fetchErr.Topic, "partition", fetchErr.Partition, "err", fetchErr.Err)
		}

		var commit []*kgo.Record
		stopped := false
		fetches.EachRecord(func(rec *kgo.Record) {
			if stopped {
				return
			}
			msgCtx := ContextFromRecord(ctx, rec.Headers)
			err := handler.Handle(msgCtx, transfer.Inbound{
				Topic:     rec.Topic,
				Partition: int(rec.Partition),
				Offset:    rec.Offset,
				Key:       append([]byte(nil), rec.Key...),
				Value:     append([]byte(nil), rec.Value...),
				At:        rec.Timestamp,
			})
			if err != nil {
				slog.ErrorContext(msgCtx, "handle message", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", err)
				stopped = true
				return
			}
			commit = append(commit, rec)
		})

		if len(commit) > 0 {
			if err := client.CommitRecords(ctx, commit...); err != nil {
				return err
			}
		}
		if stopped || (len(fetchErrs) > 0 && len(commit) == 0) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
}
