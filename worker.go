package pgoutbox

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RunPublisher drains the outbox until the context is cancelled.
//
// It blocks, so call it in a goroutine. Running several instances against the
// same database is safe and increases throughput: SKIP LOCKED guarantees each
// event is claimed once.
//
//	go pgoutbox.RunPublisher(ctx, pool, publisher, pgoutbox.Options{
//	    ServiceName: "orders-service",
//	})
func RunPublisher(ctx context.Context, pool *pgxpool.Pool, publisher Publisher, opts Options) {
	if pool == nil || publisher == nil {
		return
	}
	opts = opts.withDefaults()

	sleep := opts.PollInterval
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}

		events, err := LockPending(ctx, pool, opts.BatchSize)
		if err != nil {
			opts.logError("pgoutbox: lock pending failed", "service", opts.ServiceName, "err", err)
			sleep = opts.MaxPollInterval
			continue
		}

		if len(events) == 0 {
			// Back off while idle so an empty queue costs almost nothing.
			if sleep < opts.MaxPollInterval {
				sleep *= 2
			}
			continue
		}
		sleep = opts.PollInterval

		publishBatch(ctx, pool, publisher, opts, events)
	}
}

// publishBatch publishes one claimed batch, preserving per-key ordering.
//
// If an event fails, later events sharing its MessageKey are deferred instead
// of being published out of order. Without this a consumer could observe, say,
// "subscription cancelled" before "subscription created".
func publishBatch(ctx context.Context, pool *pgxpool.Pool, publisher Publisher, opts Options, events []PendingEvent) {
	blocked := make(map[string]struct{}, len(events))

	for _, e := range events {
		if _, isBlocked := blocked[e.MessageKey]; isBlocked {
			_ = MarkRetry(ctx, pool, e.ID,
				"deferred: an earlier event with the same key failed",
				opts.MaxAttempts, opts.MaxBackoff)
			continue
		}

		start := time.Now()
		err := publisher.Publish(ctx, e.Topic, e.MessageKey, e.Payload)
		elapsed := time.Since(start)

		if opts.OnEvent != nil {
			opts.OnEvent(Result{
				Topic:      e.Topic,
				MessageKey: e.MessageKey,
				Attempts:   e.Attempts,
				Duration:   elapsed,
				Err:        err,
			})
		}

		if err != nil {
			opts.logWarn("pgoutbox: publish failed",
				"service", opts.ServiceName, "topic", e.Topic,
				"attempts", e.Attempts, "err", err)

			_ = MarkRetry(ctx, pool, e.ID, err.Error(), opts.MaxAttempts, opts.MaxBackoff)

			if e.Attempts >= opts.MaxAttempts {
				if dlErr := SaveDeadLetter(ctx, pool, opts.ServiceName, e.Topic,
					e.MessageKey, "outbox", e.Payload, err.Error()); dlErr != nil {
					opts.logError("pgoutbox: save dead letter failed", "err", dlErr)
				}
			}

			blocked[e.MessageKey] = struct{}{}
			continue
		}

		if err := MarkPublished(ctx, pool, e.ID); err != nil {
			// The event reached the broker but the row was not updated, so it
			// will be republished. This is the at-least-once guarantee in
			// action and the reason consumers must be idempotent.
			opts.logError("pgoutbox: mark published failed",
				"service", opts.ServiceName, "id", e.ID, "err", err)
		}
	}
}
