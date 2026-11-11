package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

const insertSQL = `
WITH inserted AS (
    INSERT INTO transfers (
        id, message_key, payload, source_topic, partition, kafka_offset, status
    ) VALUES (
        $1, $2, $3::jsonb, $4, $5, $6, 'received'
    )
    ON CONFLICT DO NOTHING
    RETURNING id
)
INSERT INTO transfer_events (transfer_id, status, detail)
SELECT id, 'received', $7 FROM inserted
`

const claimPublishSQL = `
WITH picked AS (
    SELECT id
    FROM transfers
    WHERE status = 'received'
       OR (status = 'publishing' AND attempts < $3 AND locked_at <= now() - ($2::float8 * interval '1 second'))
       OR (status = 'failed' AND attempts < $3)
    ORDER BY received_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
),
updated AS (
    UPDATE transfers AS t
    SET status = 'publishing',
        attempts = t.attempts + 1,
        locked_at = now(),
        last_error = NULL
    FROM picked
    WHERE t.id = picked.id
    RETURNING t.id, t.message_key, t.payload, t.source_topic, t.partition, t.kafka_offset,
              t.status, t.attempts, t.last_error, t.locked_at, t.received_at, t.published_at
),
logged AS (
    INSERT INTO transfer_events (transfer_id, status, detail)
    SELECT id, status, 'attempt ' || attempts::text FROM updated
)
SELECT id, message_key, payload, source_topic, partition, kafka_offset,
       status, attempts, last_error, locked_at, received_at, published_at
FROM updated
`

const claimDeadSQL = `
WITH picked AS (
    SELECT id
    FROM transfers
    WHERE (status = 'failed' AND attempts >= $3)
       OR (status = 'publishing' AND attempts >= $3 AND locked_at <= now() - ($2::float8 * interval '1 second'))
    ORDER BY received_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
),
updated AS (
    UPDATE transfers AS t
    SET status = 'publishing',
        locked_at = now()
    FROM picked
    WHERE t.id = picked.id
    RETURNING t.id, t.message_key, t.payload, t.source_topic, t.partition, t.kafka_offset,
              t.status, t.attempts, t.last_error, t.locked_at, t.received_at, t.published_at
),
logged AS (
    INSERT INTO transfer_events (transfer_id, status, detail)
    SELECT id, status, 'dead-letter' FROM updated
)
SELECT id, message_key, payload, source_topic, partition, kafka_offset,
       status, attempts, last_error, locked_at, received_at, published_at
FROM updated
`

// Postgres stores transfer JSON and the outbox state.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres wraps a connection pool.
func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

// Connect retries until Postgres accepts a ping or the timeout elapses.
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		pool, err := pgxpool.New(ctx, databaseURL)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = pool.Ping(pingCtx)
			cancel()
			if err == nil {
				return pool, nil
			}
			pool.Close()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, fmt.Errorf("connect postgres: %w", err)
		case <-ticker.C:
		}
	}
}

// InsertReceived writes the Kafka message JSON. Conflicts on id or source offset are ignored.
func (s *Postgres) InsertReceived(ctx context.Context, rec transfer.Record) (bool, error) {
	if err := validateInsert(rec); err != nil {
		return false, err
	}
	tag, err := s.pool.Exec(ctx, insertSQL,
		rec.ID,
		rec.MessageKey,
		string(rec.Payload),
		rec.SourceTopic,
		rec.Partition,
		rec.Offset,
		fmt.Sprintf("%s:%d:%d", rec.SourceTopic, rec.Partition, rec.Offset),
	)
	if err != nil {
		return false, fmt.Errorf("insert transfer: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ClaimPublish locks a batch of rows that still need publishing.
func (s *Postgres) ClaimPublish(ctx context.Context, limit int, lease time.Duration, maxAttempts int) ([]transfer.Record, error) {
	return s.claim(ctx, claimPublishSQL, limit, lease, maxAttempts)
}

// ClaimDead locks a batch of rows that must be dead-lettered.
func (s *Postgres) ClaimDead(ctx context.Context, limit int, lease time.Duration, maxAttempts int) ([]transfer.Record, error) {
	return s.claim(ctx, claimDeadSQL, limit, lease, maxAttempts)
}

// MarkPublished records a successful outbound produce.
func (s *Postgres) MarkPublished(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `
		WITH updated AS (
			UPDATE transfers
			SET status = 'published', published_at = now(), locked_at = NULL, last_error = NULL
			WHERE id = $1 AND status = 'publishing'
			RETURNING id
		)
		INSERT INTO transfer_events (transfer_id, status, detail)
		SELECT id, 'published', '' FROM updated
	`, id)
	if err != nil {
		return fmt.Errorf("mark published: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errNotPublishing(id)
	}
	return nil
}

// MarkFailed records a publish error and leaves the row eligible for retry.
func (s *Postgres) MarkFailed(ctx context.Context, id, message string) error {
	tag, err := s.pool.Exec(ctx, `
		WITH updated AS (
			UPDATE transfers
			SET status = 'failed', last_error = $2, locked_at = NULL
			WHERE id = $1 AND status = 'publishing'
			RETURNING id
		)
		INSERT INTO transfer_events (transfer_id, status, detail)
		SELECT id, 'failed', $2 FROM updated
	`, id, message)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errNotPublishing(id)
	}
	return nil
}

// MarkDead records that the row was written to the dead-letter topic.
func (s *Postgres) MarkDead(ctx context.Context, id, message string) error {
	tag, err := s.pool.Exec(ctx, `
		WITH updated AS (
			UPDATE transfers
			SET status = 'dead',
			    last_error = COALESCE(NULLIF($2, ''), last_error),
			    locked_at = NULL
			WHERE id = $1 AND status = 'publishing'
			RETURNING id, last_error
		)
		INSERT INTO transfer_events (transfer_id, status, detail)
		SELECT id, 'dead', COALESCE(last_error, '') FROM updated
	`, id, message)
	if err != nil {
		return fmt.Errorf("mark dead: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errNotPublishing(id)
	}
	return nil
}

// OutboxDepth counts rows that are still in flight.
func (s *Postgres) OutboxDepth(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM transfers
		WHERE status IN ('received', 'publishing', 'failed')
	`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("outbox depth: %w", err)
	}
	return n, nil
}

// List returns the newest transfers, up to limit.
func (s *Postgres) List(ctx context.Context, limit int) ([]transfer.Record, error) {
	if limit < 1 {
		limit = 1
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, message_key, payload, source_topic, partition, kafka_offset,
		       status, attempts, last_error, locked_at, received_at, published_at
		FROM transfers
		ORDER BY received_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list transfers: %w", err)
	}
	defer rows.Close()

	out := make([]transfer.Record, 0)
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read transfers: %w", err)
	}
	return out, nil
}

// History returns one transfer and the status changes recorded for it.
func (s *Postgres) History(ctx context.Context, id string) (transfer.Record, []transfer.Event, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, message_key, payload, source_topic, partition, kafka_offset,
		       status, attempts, last_error, locked_at, received_at, published_at
		FROM transfers
		WHERE id = $1
	`, id)
	rec, err := scanRecord(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return transfer.Record{}, nil, ErrNotFound
		}
		return transfer.Record{}, nil, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT status, detail, at
		FROM transfer_events
		WHERE transfer_id = $1
		ORDER BY id
	`, id)
	if err != nil {
		return transfer.Record{}, nil, fmt.Errorf("list transfer events: %w", err)
	}
	defer rows.Close()

	events := make([]transfer.Event, 0)
	for rows.Next() {
		var event transfer.Event
		if err := rows.Scan(&event.Status, &event.Detail, &event.At); err != nil {
			return transfer.Record{}, nil, fmt.Errorf("scan transfer event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return transfer.Record{}, nil, fmt.Errorf("read transfer events: %w", err)
	}
	return rec, events, nil
}

func (s *Postgres) claim(ctx context.Context, sql string, limit int, lease time.Duration, maxAttempts int) ([]transfer.Record, error) {
	rows, err := s.pool.Query(ctx, sql, limit, lease.Seconds(), maxAttempts)
	if err != nil {
		return nil, fmt.Errorf("claim transfers: %w", err)
	}
	defer rows.Close()

	out := make([]transfer.Record, 0)
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read claimed transfers: %w", err)
	}
	return out, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanRecord(row scanner) (transfer.Record, error) {
	var rec transfer.Record
	var payload []byte
	var lastError *string
	var lockedAt *time.Time
	var publishedAt *time.Time
	err := row.Scan(
		&rec.ID,
		&rec.MessageKey,
		&payload,
		&rec.SourceTopic,
		&rec.Partition,
		&rec.Offset,
		&rec.Status,
		&rec.Attempts,
		&lastError,
		&lockedAt,
		&rec.ReceivedAt,
		&publishedAt,
	)
	if err != nil {
		return transfer.Record{}, fmt.Errorf("scan transfer: %w", err)
	}
	rec.Payload = clonePayload(payload)
	if lastError != nil {
		rec.LastError = *lastError
	}
	if lockedAt != nil {
		rec.LockedAt = *lockedAt
	}
	if publishedAt != nil {
		rec.PublishedAt = *publishedAt
	}
	return rec, nil
}
