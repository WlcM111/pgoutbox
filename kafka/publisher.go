// Package kafka provides a Kafka-backed Publisher for pgoutbox.
//
// It lives in its own package so that projects using a different broker do not
// pull in a Kafka client.
package kafka

import (
	"context"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Publisher writes outbox events to Kafka.
type Publisher struct {
	writer  *kafkago.Writer
	timeout time.Duration
}

// Config configures the Kafka publisher.
type Config struct {
	// Brokers is the bootstrap server list. Required.
	Brokers []string

	// Timeout bounds a single write. Defaults to 5s.
	Timeout time.Duration

	// Balancer chooses the partition. Defaults to &kafkago.Hash{}, which keeps
	// events sharing a key on one partition and therefore in order.
	Balancer kafkago.Balancer
}

// New creates a Kafka publisher. Close it when the application shuts down.
func New(cfg Config) (*Publisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("pgoutbox/kafka: at least one broker is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Balancer == nil {
		// Hash keeps per-key ordering, which the outbox already guarantees on
		// the producing side.
		cfg.Balancer = &kafkago.Hash{}
	}

	return &Publisher{
		writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(cfg.Brokers...),
			Balancer:               cfg.Balancer,
			AllowAutoTopicCreation: false,
			RequiredAcks:           kafkago.RequireAll,
		},
		timeout: cfg.Timeout,
	}, nil
}

// Publish implements pgoutbox.Publisher.
func (p *Publisher) Publish(ctx context.Context, topic, key string, payload []byte) error {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}

	return p.writer.WriteMessages(ctx, kafkago.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now().UTC(),
	})
}

// Close releases the underlying writer.
func (p *Publisher) Close() error {
	return p.writer.Close()
}
