package store

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/001_transfers.sql
var migration001 string

//go:embed migrations/002_events.sql
var migration002 string

const migrationLockID = int64(814271)

type migration struct {
	version string
	sql     string
}

// Migrate applies each embedded SQL file once. The transaction advisory lock lets
// the receiver, transmitter, and console start together without racing the schema change.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	steps := []migration{
		{version: "001", sql: migration001},
		{version: "002", sql: migration002},
	}
	for _, step := range steps {
		var applied bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, step.version).Scan(&applied); err != nil {
			return fmt.Errorf("read schema_migrations: %w", err)
		}
		if applied {
			continue
		}
		if _, err := tx.Exec(ctx, step.sql); err != nil {
			return fmt.Errorf("apply migration %s: %w", step.version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, step.version); err != nil {
			return fmt.Errorf("record migration %s: %w", step.version, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}
