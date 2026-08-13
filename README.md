# pgoutbox

[![CI](https://github.com/WlcM111/pgoutbox/actions/workflows/ci.yml/badge.svg)](https://github.com/WlcM111/pgoutbox/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/WlcM111/pgoutbox.svg)](https://pkg.go.dev/github.com/WlcM111/pgoutbox)
[![Go Report Card](https://goreportcard.com/badge/github.com/WlcM111/pgoutbox)](https://goreportcard.com/report/github.com/WlcM111/pgoutbox)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Transactional outbox for PostgreSQL, with an idempotency helper for consumers.

## Why

A service that writes to a database and publishes to a message broker cannot
put both in one transaction. Commit succeeds, publish fails — the event is
lost. Publish succeeds, commit rolls back — consumers act on something that
never happened.

The outbox pattern removes the problem: the event is stored in the same
database, in the same transaction as the business data. A background worker
delivers it afterwards. Either both are committed, or neither is.

## Install

```bash
go get github.com/WlcM111/pgoutbox
```

For the Kafka publisher:

```bash
go get github.com/WlcM111/pgoutbox/kafka
```

## Quick start

```go
// 1. Apply the schema once. It is idempotent.
if _, err := pool.Exec(ctx, pgoutbox.SchemaSQL); err != nil {
    return err
}

// 2. Write the event with the data that caused it.
tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)

if _, err := tx.Exec(ctx, `INSERT INTO orders (id, total) VALUES ($1, $2)`, id, total); err != nil {
    return err
}

if err := pgoutbox.AddTx(ctx, tx, pgoutbox.Event{
    AggregateType: "order",
    AggregateID:   id,
    Topic:         "orders",
    MessageKey:    id,
    EventType:     "order.created",
    Payload:       order,
}); err != nil {
    return err
}

if err := tx.Commit(ctx); err != nil {
    return err
}

// 3. Deliver in the background.
publisher, err := kafka.New(kafka.Config{Brokers: []string{"localhost:9092"}})
if err != nil {
    return err
}
go pgoutbox.RunPublisher(ctx, pool, publisher, pgoutbox.Options{
    ServiceName: "orders-service",
})
```

## Idempotent consumers

Delivery is at-least-once, so a consumer may see the same message twice. Wrap
the side effect in a transaction and let the inbox decide:

```go
tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)

first, err := pgoutbox.MarkProcessed(ctx, tx, "orders-service", msgID, "order.created")
if err != nil {
    return err
}
if !first {
    return tx.Commit(ctx) // duplicate, nothing to do
}

// apply the side effect in the same transaction
if err := applyOrder(ctx, tx, order); err != nil {
    return err
}
return tx.Commit(ctx)
```

## Guarantees

**At-least-once delivery.** An event survives a broker outage and is retried
with exponential backoff. It may be delivered more than once if the worker
crashes between publishing and recording the result — hence the inbox helper.

**Per-key ordering.** Events sharing a `MessageKey` are published in insertion
order. If one fails, later events with the same key wait, so a consumer never
sees a later state before an earlier one.

**Safe concurrency.** `FOR UPDATE SKIP LOCKED` lets several workers drain the
same queue: each event is claimed exactly once and no worker blocks on another.

**Bounded growth.** `RunCleanup` deletes delivered events and expired inbox
records. The inbox TTL must exceed the broker retention, otherwise a
redelivery could be mistaken for a new message.

## Configuration

Everything is explicit — the package never reads environment variables.

| Option | Default | Purpose |
|---|---|---|
| `ServiceName` | `pgoutbox` | identifies the publisher in logs and dead letters |
| `BatchSize` | 50 | events claimed per pass |
| `PollInterval` | 200ms | shortest pause between passes |
| `MaxPollInterval` | 2s | idle backoff cap |
| `MaxAttempts` | 20 | retries before an event is dead-lettered |
| `MaxBackoff` | 1h | retry delay cap |
| `OnEvent` | nil | hook for metrics |
| `Logger` | nil | operational logging |

## Metrics

The package does not depend on a metrics library. Use `OnEvent`:

```go
pgoutbox.Options{
    OnEvent: func(r pgoutbox.Result) {
        if r.Err != nil {
            publishFailures.WithLabelValues(r.Topic).Inc()
            return
        }
        publishDuration.WithLabelValues(r.Topic).Observe(r.Duration.Seconds())
    },
}
```

## Schema

Three tables, created by `pgoutbox.SchemaSQL`:

- `outbox_events` — pending and delivered events
- `inbox_messages` — deduplication records for consumers
- `dead_letters` — events that exhausted their retries

## Testing

Unit tests need nothing. Integration tests use
[testcontainers](https://golang.testcontainers.org/) and require Docker:

```bash
go test ./...
go test -tags=integration ./...
```

## Production use

This package powers [House VPN](https://github.com/WlcM111/vpn_system), a
Telegram-based VPN service where six Go microservices exchange events over
Kafka — payments, subscription lifecycle and node orchestration all rely on it.

## License

MIT