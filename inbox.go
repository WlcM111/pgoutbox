package pgoutbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// MarkProcessed records that a message has been handled and reports whether
// this is the first time.
//
// Because delivery is at-least-once, a consumer may receive the same message
// twice. Call this inside the transaction that applies the side effect: if it
// returns false the message is a duplicate and the transaction should commit
// without doing anything else.
//
//	tx, _ := pool.Begin(ctx)
//	defer tx.Rollback(ctx)
//
//	first, err := pgoutbox.MarkProcessed(ctx, tx, "orders-service", msgID, "order.created")
//	if err != nil {
//	    return err
//	}
//	if !first {
//	    return tx.Commit(ctx) // already handled
//	}
//
//	// apply the side effect here, in the same transaction
//	return tx.Commit(ctx)
//
// The source is part of the key, so independent services can each process the
// same message once.
func MarkProcessed(ctx context.Context, tx pgx.Tx, source, messageID, messageType string) (bool, error) {
	if source == "" || messageID == "" {
		return false, fmt.Errorf("pgoutbox: source and messageID are required")
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO inbox_messages (message_id, source, message_type)
		VALUES ($1, $2, $3)
		ON CONFLICT (message_id) DO NOTHING
	`, source+":"+messageID, source, messageType)
	if err != nil {
		return false, fmt.Errorf("pgoutbox: mark processed: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
