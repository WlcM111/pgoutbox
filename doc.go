// Package pgoutbox implements the transactional outbox pattern on PostgreSQL.
//
// # The problem
//
// A service that writes to a database and publishes to a message broker faces
// a dual-write problem: the two operations cannot share a transaction. If the
// database commit succeeds but publishing fails, the event is lost. If
// publishing succeeds but the commit is rolled back, consumers see an event
// that never happened.
//
// # The solution
//
// The outbox pattern stores outgoing events in the same database, in the same
// transaction as the business data. A separate worker reads pending events and
// publishes them to the broker. Either both the data and the event are
// committed, or neither is.
//
// Delivery is at-least-once: an event may be published more than once if the
// worker crashes between publishing and marking the row. Consumers must be
// idempotent, which is what the inbox helper in this package is for.
//
// # Usage
//
//	// 1. Apply the schema once.
//	_, err := pool.Exec(ctx, pgoutbox.SchemaSQL)
//
//	// 2. Write business data and the event in one transaction.
//	tx, _ := pool.Begin(ctx)
//	_, err = tx.Exec(ctx, "INSERT INTO orders (id, total) VALUES ($1, $2)", id, total)
//	err = pgoutbox.AddTx(ctx, tx, pgoutbox.Event{
//	    AggregateType: "order",
//	    AggregateID:   id,
//	    Topic:         "orders",
//	    MessageKey:    id,
//	    EventType:     "order.created",
//	    Payload:       order,
//	})
//	err = tx.Commit(ctx)
//
//	// 3. Run the publisher in the background.
//	go pgoutbox.RunPublisher(ctx, pool, publisher, pgoutbox.Options{
//	    ServiceName: "orders-service",
//	})
//
// # Ordering
//
// Events sharing a MessageKey are published in insertion order. If one fails,
// later events with the same key are held back until it succeeds, so a
// consumer never observes a later state before an earlier one.
package pgoutbox
