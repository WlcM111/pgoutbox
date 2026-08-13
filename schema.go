package pgoutbox

import _ "embed"

// SchemaSQL contains the DDL required by this package: the outbox table, the
// inbox deduplication table and the dead-letter table.
//
// It is idempotent (every statement uses IF NOT EXISTS), so it is safe to run
// on every start or to include in an existing migration pipeline.
//
//	if _, err := pool.Exec(ctx, pgoutbox.SchemaSQL); err != nil {
//	    return fmt.Errorf("apply pgoutbox schema: %w", err)
//	}
//
//go:embed migrations/0001_init.sql
var SchemaSQL string
