package async

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/shubhamjain/aetherbolt/internal/function"
	"time"
)

type PostgresStore struct {
	db    *sql.DB
	lease time.Duration
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db, lease: 45 * time.Second}
}
func (s *PostgresStore) Enqueue(ctx context.Context, fn function.Function, event json.RawMessage, key string) (Invocation, bool, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return Invocation{}, false, e
	}
	defer tx.Rollback()
	id := NewID()
	// ON CONFLICT keeps a replayed idempotency key inside a valid transaction;
	// a plain unique violation would abort the transaction before we could read
	// the original invocation.
	result, e := tx.ExecContext(ctx, `INSERT INTO invocations (id,function_name,version_number,event,payload_hash,idempotency_key,mode,status) VALUES ($1,$2,$3,$4,$5,NULLIF($6,''),'ASYNC','QUEUED') ON CONFLICT (function_name,version_number,idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`, id, fn.Name, fn.Version, event, Hash(event), key)
	if e != nil {
		return Invocation{}, false, e
	}
	inserted, e := result.RowsAffected()
	if e != nil {
		return Invocation{}, false, e
	}
	if inserted == 0 {
		var old Invocation
		var hash string
		e = tx.QueryRowContext(ctx, `SELECT id,payload_hash,status FROM invocations WHERE function_name=$1 AND version_number=$2 AND idempotency_key=$3`, fn.Name, fn.Version, key).Scan(&old.ID, &hash, &old.Status)
		if e != nil {
			return Invocation{}, false, e
		}
		if hash != Hash(event) {
			return Invocation{}, false, fmt.Errorf("%w: idempotency key has a different payload", function.ErrIdempotencyConflict)
		}
		return old, true, tx.Commit()
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO invocation_outbox (invocation_id) VALUES ($1)`, id)
	if e != nil {
		return Invocation{}, false, e
	}
	if e = tx.Commit(); e != nil {
		return Invocation{}, false, e
	}
	return Invocation{ID: id, FunctionName: fn.Name, Version: fn.Version, Status: "QUEUED"}, false, nil
}
func (s *PostgresStore) Claim(ctx context.Context, id string) (Invocation, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return Invocation{}, e
	}
	defer tx.Rollback()
	var inv Invocation
	// The claim predicate runs entirely in the database so one clock decides
	// whether a retry is due; comparing against the application clock is unsafe
	// when host and database clocks drift. The row lock keeps the transition
	// atomic so two consumers can never claim the same record.
	e = tx.QueryRowContext(ctx, `UPDATE invocations SET status='RUNNING', attempt_count=attempt_count+1, lease_expires_at=now()+make_interval(secs => $2), updated_at=now() WHERE id=$1 AND status IN ('QUEUED','RETRY_SCHEDULED') AND next_attempt_at <= now() RETURNING function_name,version_number,event,status,attempt_count,max_attempts`, id, s.lease.Seconds()).Scan(&inv.FunctionName, &inv.Version, &inv.Event, &inv.Status, &inv.Attempt, &inv.MaxAttempts)
	if errors.Is(e, sql.ErrNoRows) {
		var exists bool
		if e = tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM invocations WHERE id=$1)`, id).Scan(&exists); e != nil {
			return Invocation{}, e
		}
		if !exists {
			return Invocation{}, function.ErrNotFound
		}
		return Invocation{}, ErrNotClaimable
	}
	if e != nil {
		return Invocation{}, e
	}
	var ms int
	e = tx.QueryRowContext(ctx, `SELECT handler,runtime,image,timeout_ms,memory_mb,cpu_millis FROM function_versions WHERE function_name=$1 AND version_number=$2`, inv.FunctionName, inv.Version).Scan(&inv.Function.Handler, &inv.Function.Runtime, &inv.Function.Image, &ms, &inv.Function.MemoryMB, &inv.Function.CPUMillis)
	if e != nil {
		return Invocation{}, e
	}
	inv.ID = id
	inv.Function.Name = inv.FunctionName
	inv.Function.Version = inv.Version
	inv.Function.Timeout = time.Duration(ms) * time.Millisecond
	if e = tx.Commit(); e != nil {
		return Invocation{}, e
	}
	return inv, nil
}
func (s *PostgresStore) Complete(ctx context.Context, inv Invocation, result function.ExecutionResult, runErr error, delay time.Duration) error {
	status := "SUCCEEDED"
	code := ""
	var next time.Time
	decrement := 0
	if runErr != nil {
		code = failureCode(runErr)
		// Local capacity pressure or a shutdown is not the function's fault, so
		// it must not consume the retry budget and dead-letter valid work.
		if unscoredFailure(runErr) {
			decrement = 1
		}
		if inv.Attempt-decrement < inv.MaxAttempts {
			status = "RETRY_SCHEDULED"
			next = time.Now().Add(delay)
		} else {
			status = "DEAD_LETTERED"
		}
	}
	var output any
	if result.Result != nil {
		output = json.RawMessage(result.Result)
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	res, e := tx.ExecContext(ctx, `UPDATE invocations SET status=$2,result=$3,error_code=$4,attempt_count=attempt_count-$6,next_attempt_at=COALESCE(NULLIF($5,'epoch'::timestamptz),next_attempt_at),lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND status='RUNNING'`, inv.ID, status, output, code, next, decrement)
	if e != nil {
		return e
	}
	if affected, e := res.RowsAffected(); e == nil && affected == 0 {
		// Already finalized or reclaimed by another worker: do not queue outbox work.
		return tx.Commit()
	}
	switch status {
	case "RETRY_SCHEDULED":
		if _, e = tx.ExecContext(ctx, `INSERT INTO invocation_outbox (invocation_id, topic) VALUES ($1, 'INVOCATION')`, inv.ID); e != nil {
			return e
		}
	case "DEAD_LETTERED":
		if _, e = tx.ExecContext(ctx, `INSERT INTO invocation_outbox (invocation_id, topic) VALUES ($1, 'DLQ')`, inv.ID); e != nil {
			return e
		}
	}
	return tx.Commit()
}

// failureCode classifies an execution failure for operators and retry logic.
func failureCode(runErr error) string {
	switch {
	case errors.Is(runErr, function.ErrTimedOut):
		return "TIMED_OUT"
	case errors.Is(runErr, function.ErrRuntimeFailure),
		errors.Is(runErr, function.ErrCapacity),
		errors.Is(runErr, function.ErrDraining),
		errors.Is(runErr, context.Canceled):
		return "INFRASTRUCTURE_ERROR"
	default:
		return "FUNCTION_ERROR"
	}
}

// unscoredFailure reports whether a failure should not consume the retry budget.
func unscoredFailure(runErr error) bool {
	return errors.Is(runErr, function.ErrCapacity) ||
		errors.Is(runErr, function.ErrDraining) ||
		errors.Is(runErr, context.Canceled)
}

// ReclaimExpired recovers invocations whose worker lease elapsed (a crashed or
// killed worker) by returning them to the queue, or dead-lettering them when the
// retry budget is already spent, and queuing the matching outbox row atomically.
func (s *PostgresStore) ReclaimExpired(ctx context.Context) (int, error) {
	res, e := s.db.ExecContext(ctx, `WITH reclaimed AS (
		UPDATE invocations
		SET status = CASE WHEN attempt_count >= max_attempts THEN 'DEAD_LETTERED' ELSE 'RETRY_SCHEDULED' END,
		    error_code = COALESCE(NULLIF(error_code, ''), 'INFRASTRUCTURE_ERROR'),
		    lease_expires_at = NULL,
		    next_attempt_at = now(),
		    updated_at = now()
		WHERE status = 'RUNNING' AND lease_expires_at IS NOT NULL AND lease_expires_at <= now()
		RETURNING id, status
	)
	INSERT INTO invocation_outbox (invocation_id, topic)
	SELECT id, CASE WHEN status = 'DEAD_LETTERED' THEN 'DLQ' ELSE 'INVOCATION' END FROM reclaimed`)
	if e != nil {
		return 0, e
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}
func (s *PostgresStore) PendingOutbox(ctx context.Context, n int) ([]OutboxMessage, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT o.id, o.invocation_id, o.topic FROM invocation_outbox o JOIN invocations i ON i.id=o.invocation_id WHERE o.published_at IS NULL AND (o.topic = 'DLQ' OR (i.status IN ('QUEUED','RETRY_SCHEDULED') AND i.next_attempt_at <= now())) ORDER BY o.id LIMIT $1`, n)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var ids []OutboxMessage
	for rows.Next() {
		var message OutboxMessage
		if e := rows.Scan(&message.ID, &message.InvocationID, &message.Topic); e != nil {
			return nil, e
		}
		ids = append(ids, message)
	}
	return ids, rows.Err()
}
func (s *PostgresStore) MarkPublished(ctx context.Context, message OutboxMessage) error {
	_, e := s.db.ExecContext(ctx, `UPDATE invocation_outbox SET published_at=now() WHERE id=$1 AND published_at IS NULL`, message.ID)
	return e
}
func (s *PostgresStore) Get(ctx context.Context, id string) (Invocation, error) {
	var inv Invocation
	var result []byte
	err := s.db.QueryRowContext(ctx, `SELECT id,function_name,version_number,event,status,attempt_count,max_attempts,next_attempt_at,result,COALESCE(error_code,'') FROM invocations WHERE id=$1`, id).Scan(&inv.ID, &inv.FunctionName, &inv.Version, &inv.Event, &inv.Status, &inv.Attempt, &inv.MaxAttempts, &inv.NextAttemptAt, &result, &inv.ErrorCode)
	if errors.Is(err, sql.ErrNoRows) {
		return Invocation{}, function.ErrNotFound
	}
	if err != nil {
		return Invocation{}, err
	}
	if result != nil {
		inv.Result = json.RawMessage(result)
	}
	return inv, nil
}
