// Command basic demonstrates pgoutbox end to end: a business write and its
// event committed together, then delivered by the background publisher.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/WlcM111/pgoutbox"
	"github.com/jackc/pgx/v5/pgxpool"
)

type order struct {
	ID    string  `json:"id"`
	Total float64 `json:"total"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	// 1. Apply the schema. Safe to run on every start.
	if _, err := pool.Exec(ctx, pgoutbox.SchemaSQL); err != nil {
		log.Fatalf("apply schema: %v", err)
	}

	// 2. A publisher that just prints. Swap for pgoutbox/kafka in production.
	publisher := pgoutbox.PublisherFunc(
		func(_ context.Context, topic, key string, payload []byte) error {
			fmt.Printf("published topic=%s key=%s payload=%s\n", topic, key, payload)
			return nil
		},
	)

	go pgoutbox.RunPublisher(ctx, pool, publisher, pgoutbox.Options{
		ServiceName: "example",
		OnEvent: func(r pgoutbox.Result) {
			if r.Err != nil {
				fmt.Printf("publish failed topic=%s: %v\n", r.Topic, r.Err)
			}
		},
	})

	// 3. Write business data and its event in one transaction.
	o := order{ID: fmt.Sprintf("order-%d", time.Now().Unix()), Total: 99.90}

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatalf("begin: %v", err)
	}

	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, total NUMERIC NOT NULL)
	`); err != nil {
		log.Fatalf("create table: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO orders (id, total) VALUES ($1, $2)`, o.ID, o.Total); err != nil {
		log.Fatalf("insert order: %v", err)
	}

	if err := pgoutbox.AddTx(ctx, tx, pgoutbox.Event{
		AggregateType: "order",
		AggregateID:   o.ID,
		Topic:         "orders",
		MessageKey:    o.ID,
		EventType:     "order.created",
		Payload:       o,
	}); err != nil {
		log.Fatalf("add event: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("commit: %v", err)
	}

	fmt.Println("order and event committed; waiting for the publisher...")
	time.Sleep(3 * time.Second)
}
