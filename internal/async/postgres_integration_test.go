package async

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/shubhamjain/aetherbolt/internal/function"
	"github.com/shubhamjain/aetherbolt/internal/store"
)

func openTestDB(t *testing.T) *sql.DB {
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

// seedFunction creates a built-in function and its initial immutable version.
// New invocations default to max_attempts = 3.
func seedFunction(t *testing.T, db *sql.DB, name string) function.Function {
	t.Helper()
	ctx := context.Background()
	created, err := store.NewPostgresFunctionStore(db).Create(ctx, function.Function{
		Name: name, Handler: "echo", State: function.StateActive, Version: 1, Timeout: time.Second,
		Runtime: function.RuntimeBuiltin, MemoryMB: 128, CPUMillis: 250,
	})
	if err != nil {
		t.Fatalf("seed function %q: %v", name, err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = db.ExecContext(ctx, `DELETE FROM invocation_outbox WHERE invocation_id IN (SELECT id FROM invocations WHERE function_name=$1)`, name)
		_, _ = db.ExecContext(ctx, `DELETE FROM invocations WHERE function_name=$1`, name)
		_, _ = db.ExecContext(ctx, `DELETE FROM function_versions WHERE function_name=$1`, name)
		_, _ = db.ExecContext(ctx, `DELETE FROM functions WHERE name=$1`, name)
	})
	return created
}

func newFunctionName() string { return "async-it-" + NewID()[:12] }

// outboxFor returns the outbox topics recorded for a single invocation.
func outboxFor(t *testing.T, db *sql.DB, invocationID string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT topic FROM invocation_outbox WHERE invocation_id=$1 ORDER BY id`, invocationID)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()
	var topics []string
	for rows.Next() {
		var topic string
		if err := rows.Scan(&topic); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		topics = append(topics, topic)
	}
	return topics
}

func containsID(messages []OutboxMessage, id string) bool {
	for _, m := range messages {
		if m.InvocationID == id {
			return true
		}
	}
	return false
}

func TestAsyncEnqueueClaimCompleteLifecycle(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	repo := NewPostgresStore(db)
	name := newFunctionName()
	fn := seedFunction(t, db, name)

	inv, duplicate, err := repo.Enqueue(ctx, fn, json.RawMessage(`{"a":1}`), "idem-1")
	if err != nil || duplicate {
		t.Fatalf("enqueue: inv=%#v duplicate=%v err=%v", inv, duplicate, err)
	}

	stored, err := repo.Get(ctx, inv.ID)
	if err != nil || stored.Status != "QUEUED" || stored.Attempt != 0 {
		t.Fatalf("unexpected stored invocation: %#v err=%v", stored, err)
	}

	pending, err := repo.PendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("pending outbox: %v", err)
	}
	if !containsID(pending, inv.ID) {
		t.Fatalf("accepted invocation must be queued for publication")
	}
	if topics := outboxFor(t, db, inv.ID); len(topics) != 1 || topics[0] != "INVOCATION" {
		t.Fatalf("expected one INVOCATION outbox row, got %v", topics)
	}

	claimed, err := repo.Claim(ctx, inv.ID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Attempt != 1 || claimed.Function.Handler != "echo" || claimed.Function.Timeout != time.Second {
		t.Fatalf("claim did not resolve version config: %#v", claimed)
	}

	if err := repo.Complete(ctx, claimed, function.ExecutionResult{Result: json.RawMessage(`{"ok":true}`)}, nil, 0); err != nil {
		t.Fatalf("complete: %v", err)
	}

	final, err := repo.Get(ctx, inv.ID)
	if err != nil || final.Status != "SUCCEEDED" {
		t.Fatalf("unexpected final state: %#v err=%v", final, err)
	}
	// JSONB normalization can reorder/reformat the payload, so compare semantically.
	var decoded struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(final.Result, &decoded); err != nil || !decoded.OK {
		t.Fatalf("unexpected stored result: %s err=%v", final.Result, err)
	}

	pending, err = repo.PendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("pending outbox after completion: %v", err)
	}
	if containsID(pending, inv.ID) {
		t.Fatalf("a completed invocation must not remain publishable")
	}
}

func TestAsyncIdempotencyKeyDeduplicatesAndConflicts(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	repo := NewPostgresStore(db)
	fn := seedFunction(t, db, newFunctionName())

	first, duplicate, err := repo.Enqueue(ctx, fn, json.RawMessage(`{"a":1}`), "same-key")
	if err != nil || duplicate {
		t.Fatalf("first enqueue: %#v duplicate=%v err=%v", first, duplicate, err)
	}
	second, duplicate, err := repo.Enqueue(ctx, fn, json.RawMessage(`{"a":1}`), "same-key")
	if err != nil || !duplicate || second.ID != first.ID {
		t.Fatalf("replayed key must return the original invocation: second=%#v duplicate=%v err=%v", second, duplicate, err)
	}
	_, _, err = repo.Enqueue(ctx, fn, json.RawMessage(`{"a":2}`), "same-key")
	if !errors.Is(err, function.ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict for a different payload, got %v", err)
	}
}

func TestAsyncIdempotencyKeyIsOptional(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	repo := NewPostgresStore(db)
	fn := seedFunction(t, db, newFunctionName())

	first, _, err := repo.Enqueue(ctx, fn, json.RawMessage(`{"a":1}`), "")
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	second, duplicate, err := repo.Enqueue(ctx, fn, json.RawMessage(`{"a":1}`), "")
	if err != nil || duplicate || second.ID == first.ID {
		t.Fatalf("events without a key must not be deduplicated: second=%#v duplicate=%v err=%v", second, duplicate, err)
	}
}

func TestAsyncRetryThenDeadLetter(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	repo := NewPostgresStore(db)
	fn := seedFunction(t, db, newFunctionName())

	inv, _, err := repo.Enqueue(ctx, fn, json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		claimed, err := repo.Claim(ctx, inv.ID)
		if err != nil {
			t.Fatalf("claim attempt %d: %v", attempt, err)
		}
		if claimed.Attempt != attempt {
			t.Fatalf("attempt = %d, want %d", claimed.Attempt, attempt)
		}
		if err := repo.Complete(ctx, claimed, function.ExecutionResult{}, function.ErrFunctionFailed, time.Second); err != nil {
			t.Fatalf("complete attempt %d: %v", attempt, err)
		}
		stored, err := repo.Get(ctx, inv.ID)
		if err != nil {
			t.Fatalf("get attempt %d: %v", attempt, err)
		}
		if attempt < 3 {
			if stored.Status != "RETRY_SCHEDULED" {
				t.Fatalf("attempt %d status = %s, want RETRY_SCHEDULED", attempt, stored.Status)
			}
			// The republisher must not see the retry before its backoff elapses.
			pending, err := repo.PendingOutbox(ctx, 100)
			if err != nil {
				t.Fatalf("pending outbox: %v", err)
			}
			if containsID(pending, inv.ID) {
				t.Fatalf("retry must not be publishable before next_attempt_at")
			}
			if _, err := db.ExecContext(ctx, `UPDATE invocations SET next_attempt_at=now() WHERE id=$1`, inv.ID); err != nil {
				t.Fatalf("advance retry clock: %v", err)
			}
			pending, err = repo.PendingOutbox(ctx, 100)
			if err != nil {
				t.Fatalf("pending outbox after clock advance: %v", err)
			}
			if !containsID(pending, inv.ID) {
				t.Fatalf("due retry must be publishable")
			}
		} else {
			if stored.Status != "DEAD_LETTERED" || stored.ErrorCode != "FUNCTION_ERROR" {
				t.Fatalf("exhausted invocation = %#v, want DEAD_LETTERED/FUNCTION_ERROR", stored)
			}
			var invocations, dlq int
			for _, topic := range outboxFor(t, db, inv.ID) {
				switch topic {
				case "INVOCATION":
					invocations++
				case "DLQ":
					dlq++
				}
			}
			if dlq != 1 || invocations != 3 {
				t.Fatalf("expected 3 INVOCATION and 1 DLQ outbox rows, got invocations=%d dlq=%d", invocations, dlq)
			}
			pending, err := repo.PendingOutbox(ctx, 100)
			if err != nil {
				t.Fatalf("pending outbox: %v", err)
			}
			if !containsID(pending, inv.ID) {
				t.Fatalf("dead-lettered invocation must be published to the DLQ")
			}
		}
	}
}

func TestAsyncClaimGuards(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	repo := NewPostgresStore(db)
	fn := seedFunction(t, db, newFunctionName())

	if _, err := repo.Claim(ctx, "does-not-exist"); !errors.Is(err, function.ErrNotFound) {
		t.Fatalf("expected not-found for an unknown id, got %v", err)
	}

	inv, _, err := repo.Enqueue(ctx, fn, json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE invocations SET next_attempt_at=now() + interval '1 hour' WHERE id=$1`, inv.ID); err != nil {
		t.Fatalf("defer invocation: %v", err)
	}
	if _, err := repo.Claim(ctx, inv.ID); !errors.Is(err, ErrNotClaimable) {
		t.Fatalf("expected ErrNotClaimable before the backoff elapses, got %v", err)
	}

	if _, err := db.ExecContext(ctx, `UPDATE invocations SET next_attempt_at=now() WHERE id=$1`, inv.ID); err != nil {
		t.Fatalf("enable invocation: %v", err)
	}
	claimed, err := repo.Claim(ctx, inv.ID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := repo.Complete(ctx, claimed, function.ExecutionResult{Result: json.RawMessage(`{}`)}, nil, 0); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, err := repo.Claim(ctx, inv.ID); !errors.Is(err, ErrNotClaimable) {
		t.Fatalf("expected ErrNotClaimable for a terminal invocation, got %v", err)
	}
}

func TestAsyncConcurrentClaimHasSingleWinner(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	repo := NewPostgresStore(db)
	fn := seedFunction(t, db, newFunctionName())

	inv, _, err := repo.Enqueue(ctx, fn, json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	const racers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]error, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, err := repo.Claim(ctx, inv.ID)
			results[idx] = err
		}(i)
	}
	close(start)
	wg.Wait()

	var winners, notClaimable int
	for _, err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrNotClaimable):
			notClaimable++
		default:
			t.Fatalf("unexpected claim error: %v", err)
		}
	}
	if winners != 1 || notClaimable != racers-1 {
		t.Fatalf("expected exactly one winner, got winners=%d notClaimable=%d", winners, notClaimable)
	}
}

func TestAsyncInvocationPinnedToAcceptedVersion(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	repo := NewPostgresStore(db)
	name := newFunctionName()
	fn := seedFunction(t, db, name) // version 1, handler "echo"

	inv, _, err := repo.Enqueue(ctx, fn, json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// A deployment after acceptance must not change what the invocation runs.
	deployed, err := store.NewPostgresFunctionStore(db).Deploy(ctx, name, function.Function{
		Handler: "uppercase", Runtime: function.RuntimeBuiltin, Timeout: time.Second, MemoryMB: 128, CPUMillis: 250,
	})
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if deployed.Version != 2 {
		t.Fatalf("expected version 2 after deploy, got %#v", deployed)
	}

	claimed, err := repo.Claim(ctx, inv.ID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Version != 1 || claimed.Function.Handler != "echo" {
		t.Fatalf("accepted invocation must stay pinned to version 1, got %#v", claimed)
	}
}

func countOutboxTopic(t *testing.T, db *sql.DB, invocationID, topic string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM invocation_outbox WHERE invocation_id=$1 AND topic=$2`, invocationID, topic).Scan(&n); err != nil {
		t.Fatalf("count outbox topic: %v", err)
	}
	return n
}

func TestAsyncReclaimRecoversStuckInvocation(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	repo := NewPostgresStore(db)
	repo.lease = 40 * time.Millisecond
	fn := seedFunction(t, db, newFunctionName())

	inv, _, err := repo.Enqueue(ctx, fn, json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := repo.Claim(ctx, inv.ID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", claimed.Attempt)
	}

	// Simulate a worker that died after claiming: leave the row RUNNING and let
	// the lease elapse.
	time.Sleep(200 * time.Millisecond)
	reclaimed, err := repo.ReclaimExpired(ctx)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if reclaimed < 1 {
		t.Fatalf("expected at least one reclaimed invocation, got %d", reclaimed)
	}

	recovered, err := repo.Get(ctx, inv.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if recovered.Status != "RETRY_SCHEDULED" || recovered.ErrorCode != "INFRASTRUCTURE_ERROR" {
		t.Fatalf("recovered invocation = %#v, want RETRY_SCHEDULED/INFRASTRUCTURE_ERROR", recovered)
	}
	if countOutboxTopic(t, db, inv.ID, "INVOCATION") != 2 {
		t.Fatalf("expected the acceptance and reclaim outbox rows for the invocation")
	}
	if _, err := repo.Claim(ctx, inv.ID); err != nil {
		t.Fatalf("reclaimed invocation must be claimable again: %v", err)
	}
}

func TestAsyncReclaimDeadLettersExhaustedInvocation(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	repo := NewPostgresStore(db)
	fn := seedFunction(t, db, newFunctionName())

	inv, _, err := repo.Enqueue(ctx, fn, json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := repo.Claim(ctx, inv.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// A crash on the final attempt plus an elapsed lease must dead-letter.
	if _, err := db.ExecContext(ctx, `UPDATE invocations SET attempt_count=max_attempts, lease_expires_at=now()-interval '1 second' WHERE id=$1`, inv.ID); err != nil {
		t.Fatalf("age invocation: %v", err)
	}
	if _, err := repo.ReclaimExpired(ctx); err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	recovered, err := repo.Get(ctx, inv.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if recovered.Status != "DEAD_LETTERED" {
		t.Fatalf("status = %s, want DEAD_LETTERED", recovered.Status)
	}
	if countOutboxTopic(t, db, inv.ID, "DLQ") != 1 {
		t.Fatal("exhausted reclaimed invocation must queue a DLQ outbox row")
	}
}

func TestAsyncCapacityFailurePreservesRetryBudget(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	repo := NewPostgresStore(db)
	fn := seedFunction(t, db, newFunctionName())

	inv, _, err := repo.Enqueue(ctx, fn, json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := repo.Claim(ctx, inv.ID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := repo.Complete(ctx, claimed, function.ExecutionResult{}, function.ErrCapacity, 0); err != nil {
		t.Fatalf("complete: %v", err)
	}
	stored, err := repo.Get(ctx, inv.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stored.Status != "RETRY_SCHEDULED" || stored.ErrorCode != "INFRASTRUCTURE_ERROR" {
		t.Fatalf("capacity failure = %#v, want RETRY_SCHEDULED/INFRASTRUCTURE_ERROR", stored)
	}
	if stored.Attempt != 0 {
		t.Fatalf("capacity failure must not consume the retry budget, attempt = %d", stored.Attempt)
	}
	if _, err := db.ExecContext(ctx, `UPDATE invocations SET next_attempt_at=now() WHERE id=$1`, inv.ID); err != nil {
		t.Fatalf("advance clock: %v", err)
	}
	again, err := repo.Claim(ctx, inv.ID)
	if err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	if again.Attempt != 1 {
		t.Fatalf("re-claim attempt = %d, want 1 (budget preserved)", again.Attempt)
	}
}
