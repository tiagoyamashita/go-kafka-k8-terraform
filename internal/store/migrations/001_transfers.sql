CREATE TABLE IF NOT EXISTS transfers (
    id TEXT PRIMARY KEY,
    message_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    source_topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    locked_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    CONSTRAINT transfers_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT transfers_status CHECK (status IN ('received', 'publishing', 'published', 'failed', 'dead')),
    CONSTRAINT transfers_source_offset UNIQUE (source_topic, partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS transfers_outbox_idx ON transfers (status, received_at);
