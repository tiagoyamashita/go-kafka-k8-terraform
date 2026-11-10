CREATE TABLE IF NOT EXISTS transfer_events (
    id BIGSERIAL PRIMARY KEY,
    transfer_id TEXT NOT NULL REFERENCES transfers (id),
    status TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS transfer_events_transfer_idx ON transfer_events (transfer_id, id);
