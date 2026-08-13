package pgoutbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is an outgoing message stored alongside the business data that
// produced it.
type Event struct {
	// AggregateType names the entity the event belongs to, e.g. "order".
	AggregateType string

	// AggregateID identifies that entity.
	AggregateID string

	// Topic is the broker destination.
	Topic string

	// MessageKey controls ordering and partitioning. Events sharing a key are
	// published in insertion order.
	MessageKey string

	// EventType names the event, e.g. "order.created".
	EventType string

	// Payload is marshalled to JSON before storage.
	Payload any
}

// AddTx stores an event inside the caller's transaction.
//
// This is the heart of the pattern: because the event is written in the same
// transaction as the business data, the two either both commit or both roll
// back, and no event can describe a state that was never persisted.
func AddTx(ctx context.Context, tx pgx.Tx, e Event) error {
	if e.Topic == "" {
		return fmt.Errorf("pgoutbox: event topic is required")
	}

	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return fmt.Errorf("pgoutbox: marshal payload: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (
			aggregate_type, aggregate_id, topic, message_key, event_type, payload
		) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
	`, e.AggregateType, e.AggregateID, e.Topic, e.MessageKey, e.EventType, string(payload))
	if err != nil {
		return fmt.Errorf("pgoutbox: insert event: %w", err)
	}
	return nil
}

// PendingEvent is an event claimed for publishing.
type PendingEvent struct {
	ID         int64
	Topic      string
	MessageKey string
	Payload    []byte
	Attempts   int
}

// LockPending claims up to limit events for publishing and marks them as being
// processed.
//
// FOR UPDATE SKIP LOCKED lets several workers drain the same queue
// concurrently: each row is handed to exactly one worker, and no worker waits
// on another's lock.
func LockPending(ctx context.Context, pool *pgxpool.Pool, limit int) ([]PendingEvent, error) {
	if limit <= 0 {
		limit = 50
	}

	rows, err := pool.Query(ctx, `
		WITH picked AS (
			SELECT id
			FROM outbox_events
			WHERE status IN ('pending', 'retry')
			  AND next_attempt_at <= now()
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events o
		SET status = 'processing', attempts = attempts + 1
		FROM picked
		WHERE o.id = picked.id
		RETURNING o.id, o.topic, o.message_key, o.payload, o.attempts
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("pgoutbox: lock pending: %w", err)
	}
	defer rows.Close()

	var out []PendingEvent
	for rows.Next() {
		var e PendingEvent
		if err := rows.Scan(&e.ID, &e.Topic, &e.MessageKey, &e.Payload, &e.Attempts); err != nil {
			return nil, fmt.Errorf("pgoutbox: scan pending: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MarkPublished records a successful delivery.
func MarkPublished(ctx context.Context, pool *pgxpool.Pool, id int64) error {
	_, err := pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'published', published_at = now(), last_error = ''
		WHERE id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("pgoutbox: mark published: %w", err)
	}
	return nil
}

// MarkRetry schedules another attempt with exponential backoff, or gives up
// once maxAttempts is reached.
//
// The delay doubles with each attempt and is capped at maxBackoff, so a broker
// outage does not turn into a tight retry loop.
func MarkRetry(ctx context.Context, pool *pgxpool.Pool, id int64, errText string, maxAttempts int, maxBackoff time.Duration) error {
	_, err := pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = CASE WHEN attempts >= $3 THEN 'failed' ELSE 'retry' END,
		    next_attempt_at = CASE
		        WHEN attempts >= $3 THEN next_attempt_at
		        ELSE now() + (LEAST($4, POWER(2, LEAST(attempts, 16))::bigint) || ' seconds')::interval
		    END,
		    last_error = $2
		WHERE id = $1
	`, id, errText, maxAttempts, int64(maxBackoff.Seconds()))
	if err != nil {
		return fmt.Errorf("pgoutbox: mark retry: %w", err)
	}
	return nil
}

// SaveDeadLetter copies an event that exhausted its retries into dead_letters
// so it can be inspected and replayed manually.
func SaveDeadLetter(ctx context.Context, pool *pgxpool.Pool, source, topic, key, messageType string, payload []byte, errText string) error {
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	if !json.Valid(payload) {
		wrapped, _ := json.Marshal(map[string]string{"raw": string(payload)})
		payload = wrapped
	}

	_, err := pool.Exec(ctx, `
		INSERT INTO dead_letters (source, topic, message_key, message_type, payload, error)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6)
	`, source, topic, key, messageType, string(payload), errText)
	if err != nil {
		return fmt.Errorf("pgoutbox: save dead letter: %w", err)
	}
	return nil
}
