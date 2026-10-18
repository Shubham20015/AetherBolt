package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/shubhamjain/aetherbolt/internal/function"
)

func TestPostgresFunctionStore(t *testing.T) {
	databaseURL := os.Getenv("AETHERBOLT_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set AETHERBOLT_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}

	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	db := sql.OpenDB(stdlib.GetConnector(*config))
	t.Cleanup(func() { db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	store := NewPostgresFunctionStore(db)
	const name = "postgres-integration-test"
	// Delete in dependency order so the function_versions FK (added in 000003)
	// does not block cleanup.
	clean := func() {
		ctx := context.Background()
		_, _ = db.ExecContext(ctx, `DELETE FROM invocation_outbox WHERE invocation_id IN (SELECT id FROM invocations WHERE function_name=$1)`, name)
		_, _ = db.ExecContext(ctx, `DELETE FROM invocations WHERE function_name=$1`, name)
		_, _ = db.ExecContext(ctx, `DELETE FROM function_versions WHERE function_name=$1`, name)
		_, _ = db.ExecContext(ctx, `DELETE FROM functions WHERE name=$1`, name)
	}
	clean()
	t.Cleanup(clean)

	created, err := store.Create(ctx, function.Function{
		Name: name, Handler: "echo", State: function.StateActive, Version: 1, Timeout: time.Second,
		Runtime: function.RuntimeBuiltin, MemoryMB: 128, CPUMillis: 250,
	})
	if err != nil {
		t.Fatalf("create function: %v", err)
	}
	if created.CreatedAt.IsZero() || created.Timeout != time.Second {
		t.Fatalf("unexpected created function: %#v", created)
	}

	found, err := store.Get(ctx, name)
	if err != nil {
		t.Fatalf("get function: %v", err)
	}
	if found.Name != name || found.Handler != "echo" || found.State != function.StateActive {
		t.Fatalf("unexpected stored function: %#v", found)
	}

	_, err = store.Create(ctx, created)
	if !errors.Is(err, function.ErrAlreadyExists) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}
