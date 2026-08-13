-- pgoutbox schema.
--
-- outbox_events    — outgoing events, written in the business transaction
-- inbox_messages   — consumed message ids, for idempotent processing
-- dead_letters     — events that exhausted their retries

CREATE TABLE IF NOT EXISTS outbox_events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    TEXT        NOT NULL,
    topic           TEXT        NOT NULL,
    message_key     TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending',
    attempts        INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ NULL,

    CONSTRAINT outbox_events_status_check
        CHECK (status IN ('pending', 'processing', 'retry', 'published', 'failed'))
);

-- Partial index: the publisher only ever scans rows waiting to be sent, so the
-- index stays small no matter how many published rows accumulate.
CREATE INDEX IF NOT EXISTS idx_outbox_events_pending
    ON outbox_events (status, next_attempt_at, id)
    WHERE status IN ('pending', 'retry');

CREATE INDEX IF NOT EXISTS idx_outbox_events_published
    ON outbox_events (published_at)
    WHERE status = 'published';

CREATE TABLE IF NOT EXISTS inbox_messages (
    message_id   TEXT        PRIMARY KEY,
    source       TEXT        NOT NULL,
    message_type TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_inbox_messages_processed_at
    ON inbox_messages (processed_at);

CREATE TABLE IF NOT EXISTS dead_letters (
    id           BIGSERIAL   PRIMARY KEY,
    source       TEXT        NOT NULL,
    topic        TEXT        NOT NULL DEFAULT '',
    message_key  TEXT        NOT NULL DEFAULT '',
    message_type TEXT        NOT NULL DEFAULT '',
    payload      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    error        TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_created_at
    ON dead_letters (created_at DESC);