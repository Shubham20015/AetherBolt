package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/shubhamjain/aetherbolt/internal/function"
)

func openVersionTestDB(t *testing.T) *sql.DB {
	t.Helper()
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
	return db
}

func uniqueFunctionName() string {
	raw := make([]byte, 6)
	_, _ = rand.Read(raw)
	return "version-it-" + hex.EncodeToString(raw)
}

func cleanupVersionFixture(t *testing.T, db *sql.DB, name string) {
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = db.ExecContext(ctx, `DELETE FROM invocation_outbox WHERE invocation_id IN (SELECT id FROM invocations WHERE function_name=$1)`, name)
		_, _ = db.ExecContext(ctx, `DELETE FROM invocations WHERE function_name=$1`, name)
		_, _ = db.ExecContext(ctx, `DELETE FROM function_versions WHERE function_name=$1`, name)
		_, _ = db.ExecContext(ctx, `DELETE FROM functions WHERE name=$1`, name)
	})
}

func candidate(handler string) function.Function {
	return function.Function{Handler: handler, Runtime: function.RuntimeBuiltin, Timeout: time.Second, MemoryMB: 128, CPUMillis: 250}
}

func activeVersionStatuses(t *testing.T, db *sql.DB, name string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT status FROM function_versions WHERE function_name=$1 ORDER BY version_number`, name)
	if err != nil {
		t.Fatalf("query versions: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			t.Fatalf("scan version: %v", err)
		}
		out = append(out, status)
	}
	return out
}

func TestPostgresDeployAndRollback(t *testing.T) {
	db := openVersionTestDB(t)
	ctx := context.Background()
	repo := NewPostgresFunctionStore(db)
	name := uniqueFunctionName()
	cleanupVersionFixture(t, db, name)

	created, err := repo.Create(ctx, function.Function{Name: name, Handler: "echo", State: function.StateActive, Version: 1, Timeout: time.Second, Runtime: function.RuntimeBuiltin, MemoryMB: 128, CPUMillis: 250})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if statuses := activeVersionStatuses(t, db, name); len(statuses) != 1 || statuses[0] != "ACTIVE" {
		t.Fatalf("expected one ACTIVE version after create, got %v", statuses)
	}

	v2, err := repo.Deploy(ctx, name, candidate("uppercase"))
	if err != nil {
		t.Fatalf("deploy v2: %v", err)
	}
	if v2.Version != 2 || v2.Handler != "uppercase" {
		t.Fatalf("unexpected v2: %#v", v2)
	}
	v3, err := repo.Deploy(ctx, name, candidate("echo"))
	if err != nil {
		t.Fatalf("deploy v3: %v", err)
	}
	if v3.Version != 3 {
		t.Fatalf("unexpected v3: %#v", v3)
	}
	if statuses := activeVersionStatuses(t, db, name); len(statuses) != 3 || statuses[0] != "RETIRED" || statuses[1] != "RETIRED" || statuses[2] != "ACTIVE" {
		t.Fatalf("expected v3 ACTIVE with v1/v2 RETIRED, got %v", statuses)
	}

	rolled, err := repo.Rollback(ctx, name, 1)
	if err != nil {
		t.Fatalf("rollback to v1: %v", err)
	}
	if rolled.Version != 1 || rolled.Handler != "echo" {
		t.Fatalf("unexpected rollback result: %#v", rolled)
	}
	if statuses := activeVersionStatuses(t, db, name); statuses[0] != "ACTIVE" || statuses[2] != "RETIRED" {
		t.Fatalf("expected v1 ACTIVE and v3 RETIRED after rollback, got %v", statuses)
	}
	if _, err := repo.Rollback(ctx, name, 99); err == nil {
		t.Fatal("expected rollback of a missing version to fail")
	}
	_ = created
}

func TestPostgresConcurrentDeployAllocatesUniqueVersions(t *testing.T) {
	db := openVersionTestDB(t)
	ctx := context.Background()
	repo := NewPostgresFunctionStore(db)
	name := uniqueFunctionName()
	cleanupVersionFixture(t, db, name)

	if _, err := repo.Create(ctx, function.Function{Name: name, Handler: "echo", State: function.StateActive, Version: 1, Timeout: time.Second, Runtime: function.RuntimeBuiltin, MemoryMB: 128, CPUMillis: 250}); err != nil {
		t.Fatalf("create: %v", err)
	}

	const deployments = 6
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, deployments)
	versions := make([]int, deployments)
	for i := 0; i < deployments; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			fn, err := repo.Deploy(ctx, name, candidate("echo"))
			errs[idx], versions[idx] = err, fn.Version
		}(i)
	}
	close(start)
	wg.Wait()

	seen := map[int]bool{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("deployment %d failed: %v", i, err)
		}
		if versions[i] < 2 || versions[i] > deployments+1 {
			t.Fatalf("deployment %d produced out-of-range version %d", i, versions[i])
		}
		if seen[versions[i]] {
			t.Fatalf("duplicate version %d allocated by concurrent deployments", versions[i])
		}
		seen[versions[i]] = true
	}
	if len(seen) != deployments {
		t.Fatalf("expected %d distinct versions, got %d", deployments, len(seen))
	}

	// Exactly one active version must remain after the storm.
	rows, err := db.QueryContext(ctx, `SELECT count(*) FROM function_versions WHERE function_name=$1 AND status='ACTIVE'`, name)
	if err != nil {
		t.Fatalf("count active: %v", err)
	}
	defer rows.Close()
	rows.Next()
	var active int
	if err := rows.Scan(&active); err != nil {
		t.Fatalf("scan active: %v", err)
	}
	if active != 1 {
		t.Fatalf("expected exactly one ACTIVE version, got %d", active)
	}
	current, err := repo.Get(ctx, name)
	if err != nil || current.Version != deployments+1 {
		t.Fatalf("active version = %#v err=%v, want %d", current, err, deployments+1)
	}
}
