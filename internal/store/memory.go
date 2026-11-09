package store

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

// Memory is the in-memory stand-in for the Postgres outbox. Tests use it to
// exercise idempotent inserts and claim, retry, and dead-letter transitions.
type Memory struct {
	mu      sync.Mutex
	now     func() time.Time
	rows    map[string]transfer.Record
	offsets map[string]string
}

// NewMemory returns an empty store.
func NewMemory() *Memory {
	return &Memory{
		now:     time.Now,
		rows:    make(map[string]transfer.Record),
		offsets: make(map[string]string),
	}
}

// SetNow replaces the clock used for leases and timestamps.
func (m *Memory) SetNow(now func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = now
}

// InsertReceived stores the JSON payload once. A repeated id or source offset is a no-op.
func (m *Memory) InsertReceived(_ context.Context, rec transfer.Record) (bool, error) {
	if err := validateInsert(rec); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.rows[rec.ID]; ok {
		return false, nil
	}
	key := offsetKey(rec.SourceTopic, rec.Partition, rec.Offset)
	if _, ok := m.offsets[key]; ok {
		return false, nil
	}
	rec.Payload = clonePayload(rec.Payload)
	rec.Status = transfer.StatusReceived
	rec.Attempts = 0
	rec.LastError = ""
	rec.LockedAt = time.Time{}
	rec.PublishedAt = time.Time{}
	rec.ReceivedAt = m.now()
	m.rows[rec.ID] = rec
	m.offsets[key] = rec.ID
	return true, nil
}

// ClaimPublish locks rows that still need an outbound publish and increments attempts.
func (m *Memory) ClaimPublish(_ context.Context, limit int, lease time.Duration, maxAttempts int) ([]transfer.Record, error) {
	return m.claim(limit, true, func(rec transfer.Record, now time.Time) bool {
		switch rec.Status {
		case transfer.StatusReceived:
			return true
		case transfer.StatusPublishing:
			return rec.Attempts < maxAttempts && leaseExpired(rec, now, lease)
		case transfer.StatusFailed:
			return rec.Attempts < maxAttempts
		default:
			return false
		}
	})
}

// ClaimDead locks rows that have exhausted publish attempts so they can be sent to the DLQ.
func (m *Memory) ClaimDead(_ context.Context, limit int, lease time.Duration, maxAttempts int) ([]transfer.Record, error) {
	return m.claim(limit, false, func(rec transfer.Record, now time.Time) bool {
		switch rec.Status {
		case transfer.StatusFailed:
			return rec.Attempts >= maxAttempts
		case transfer.StatusPublishing:
			return rec.Attempts >= maxAttempts && leaseExpired(rec, now, lease)
		default:
			return false
		}
	})
}

// MarkPublished completes a successful publish.
func (m *Memory) MarkPublished(_ context.Context, id string) error {
	return m.mark(id, func(rec *transfer.Record) {
		rec.Status = transfer.StatusPublished
		rec.PublishedAt = m.now()
		rec.LockedAt = time.Time{}
		rec.LastError = ""
	})
}

// MarkFailed returns a row to the retry queue.
func (m *Memory) MarkFailed(_ context.Context, id, message string) error {
	return m.mark(id, func(rec *transfer.Record) {
		rec.Status = transfer.StatusFailed
		rec.LastError = message
		rec.LockedAt = time.Time{}
	})
}

// MarkDead stops further publish attempts.
func (m *Memory) MarkDead(_ context.Context, id, message string) error {
	return m.mark(id, func(rec *transfer.Record) {
		rec.Status = transfer.StatusDead
		if message != "" {
			rec.LastError = message
		}
		rec.LockedAt = time.Time{}
	})
}

// OutboxDepth counts rows that are not published or dead.
func (m *Memory) OutboxDepth(context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, rec := range m.rows {
		switch rec.Status {
		case transfer.StatusReceived, transfer.StatusPublishing, transfer.StatusFailed:
			n++
		}
	}
	return n, nil
}

// Get returns a copy of one row.
func (m *Memory) Get(id string) (transfer.Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.rows[id]
	if !ok {
		return transfer.Record{}, false
	}
	rec.Payload = clonePayload(rec.Payload)
	return rec, true
}

// Len returns the number of stored transfers.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rows)
}

func (m *Memory) claim(limit int, bump bool, match func(transfer.Record, time.Time) bool) ([]transfer.Record, error) {
	if limit < 1 {
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	picked := make([]transfer.Record, 0)
	for _, rec := range m.rows {
		if match(rec, now) {
			picked = append(picked, rec)
		}
	}
	sort.Slice(picked, func(i, j int) bool {
		return lessRecord(picked[i], picked[j])
	})
	if len(picked) > limit {
		picked = picked[:limit]
	}
	out := make([]transfer.Record, 0, len(picked))
	for _, rec := range picked {
		current := m.rows[rec.ID]
		current.Status = transfer.StatusPublishing
		current.LockedAt = now
		if bump {
			current.Attempts++
			current.LastError = ""
		}
		m.rows[rec.ID] = current
		current.Payload = clonePayload(current.Payload)
		out = append(out, current)
	}
	return out, nil
}

func (m *Memory) mark(id string, update func(*transfer.Record)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.rows[id]
	if !ok {
		return errNotPublishing(id)
	}
	if rec.Status != transfer.StatusPublishing {
		return errNotPublishing(id)
	}
	update(&rec)
	m.rows[id] = rec
	return nil
}

func leaseExpired(rec transfer.Record, now time.Time, lease time.Duration) bool {
	if rec.LockedAt.IsZero() {
		return true
	}
	return !rec.LockedAt.After(now.Add(-lease))
}

func lessRecord(a, b transfer.Record) bool {
	if a.ReceivedAt.Equal(b.ReceivedAt) {
		return a.ID < b.ID
	}
	return a.ReceivedAt.Before(b.ReceivedAt)
}
