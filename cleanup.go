package pgoutbox

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CleanupOptions configures retention of processed rows.
type CleanupOptions struct {
	// Interval between cleanup passes. Defaults to 1h.
	Interval time.Duration

	// PublishedTTL is how long delivered events are kept. Defaults to 7 days.
	PublishedTTL time.Duration

	// InboxTTL is how long deduplication records are kept.
	//
	// This must exceed the broker's retention period. If a record expires
	// while the message is still replayable, a redelivery would be treated as
	// new and the side effect applied twice. Defaults to 30 days, comfortably
	// above the usual 7-day Kafka retention.
	InboxTTL time.Duration

	// Logger, if set, receives operational messages.
	Logger Logger
}

func (o CleanupOptions) withDefaults() CleanupOptions {
	if o.Interval <= 0 {
		o.Interval = time.Hour
	}
	if o.PublishedTTL <= 0 {
		o.PublishedTTL = 7 * 24 * time.Hour
	}
	if o.InboxTTL <= 0 {
		o.InboxTTL = 30 * 24 * time.Hour
	}
	return o
}

// RunCleanup periodically deletes delivered events and expired deduplication
// records. Without it both tables grow without bound, bloating indexes and
// slowing vacuum.
//
// It blocks, so call it in a goroutine. Running it in a single service is
// enough; running it in several is harmless because the deletes are idempotent.
func RunCleanup(ctx context.Context, pool *pgxpool.Pool, opts CleanupOptions) {
	if pool == nil {
		return
	}
	opts = opts.withDefaults()

	cleanupOnce(ctx, pool, opts)

	ticker := time.NewTicker(opts.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleanupOnce(ctx, pool, opts)
		}
	}
}

func cleanupOnce(ctx context.Context, pool *pgxpool.Pool, opts CleanupOptions) {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if _, err := pool.Exec(cctx, `
		DELETE FROM outbox_events
		WHERE status = 'published'
		  AND published_at < now() - make_interval(secs => $1)
	`, opts.PublishedTTL.Seconds()); err != nil && opts.Logger != nil {
		opts.Logger.Error("pgoutbox: cleanup outbox failed", "err", err)
	}

	if _, err := pool.Exec(cctx, `
		DELETE FROM inbox_messages
		WHERE processed_at < now() - make_interval(secs => $1)
	`, opts.InboxTTL.Seconds()); err != nil && opts.Logger != nil {
		opts.Logger.Error("pgoutbox: cleanup inbox failed", "err", err)
	}
}
