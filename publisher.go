package pgoutbox

import (
	"context"
	"time"
)

// Publisher delivers a single event to a message broker.
//
// Implementing this interface is the only integration point required by this
// package. A Kafka implementation is provided in the kafka subpackage; keeping
// it separate means users of other brokers do not pull in a Kafka client.
//
// Publish must be safe for concurrent use and should honour the context
// deadline. Returning an error schedules a retry with exponential backoff.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

// PublisherFunc adapts an ordinary function to the Publisher interface.
type PublisherFunc func(ctx context.Context, topic, key string, payload []byte) error

// Publish implements Publisher.
func (f PublisherFunc) Publish(ctx context.Context, topic, key string, payload []byte) error {
	return f(ctx, topic, key, payload)
}

// Options configures the background workers.
//
// The zero value is usable: every field falls back to a sensible default.
// Configuration is explicit rather than read from the environment, so the
// calling application stays in control of where its settings come from.
type Options struct {
	// ServiceName identifies the publisher in logs and dead letters.
	// Defaults to "pgoutbox".
	ServiceName string

	// BatchSize is how many events one pass claims. Larger batches raise
	// throughput under load at the cost of longer individual passes.
	// Defaults to 50.
	BatchSize int

	// PollInterval is the shortest pause between passes. When the queue is
	// empty the interval backs off up to MaxPollInterval.
	// Defaults to 200ms.
	PollInterval time.Duration

	// MaxPollInterval caps the idle backoff. Defaults to 2s.
	MaxPollInterval time.Duration

	// MaxAttempts is how many times an event is retried before it is marked
	// failed and copied to dead_letters. Defaults to 20.
	MaxAttempts int

	// MaxBackoff caps the delay between retries. Defaults to 1h.
	MaxBackoff time.Duration

	// OnEvent, if set, is called after every publish attempt. Use it to feed
	// metrics without this package depending on a metrics library.
	OnEvent func(Result)

	// Logger, if set, receives operational messages. Defaults to no logging,
	// so the package stays silent unless asked.
	Logger Logger
}

// Result describes the outcome of a single publish attempt.
type Result struct {
	Topic      string
	MessageKey string
	Attempts   int
	Duration   time.Duration
	Err        error
}

// Logger is the minimal logging surface this package needs. It is satisfied by
// a thin wrapper around log/slog, zap, zerolog or the standard log package.
type Logger interface {
	Error(msg string, args ...any)
	Warn(msg string, args ...any)
}

func (o Options) withDefaults() Options {
	if o.ServiceName == "" {
		o.ServiceName = "pgoutbox"
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 50
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 200 * time.Millisecond
	}
	if o.MaxPollInterval <= 0 {
		o.MaxPollInterval = 2 * time.Second
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 20
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = time.Hour
	}
	return o
}

func (o Options) logError(msg string, args ...any) {
	if o.Logger != nil {
		o.Logger.Error(msg, args...)
	}
}

func (o Options) logWarn(msg string, args ...any) {
	if o.Logger != nil {
		o.Logger.Warn(msg, args...)
	}
}
