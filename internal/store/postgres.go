package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/shubhamjain/aetherbolt/internal/function"
)

type PostgresFunctionStore struct{ db *sql.DB }

func NewPostgresFunctionStore(db *sql.DB) *PostgresFunctionStore {
	return &PostgresFunctionStore{db: db}
}

func (s *PostgresFunctionStore) Create(ctx context.Context, input function.Function) (function.Function, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return function.Function{}, fmt.Errorf("begin create function: %w", err)
	}
	defer tx.Rollback()
	const query = `INSERT INTO functions (name, handler, description, state, active_version, timeout_ms, runtime, image, memory_mb, cpu_millis)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	RETURNING name, handler, description, state, active_version, timeout_ms, runtime, image, memory_mb, cpu_millis, created_at`
	var out function.Function
	var timeoutMS int
	err = tx.QueryRowContext(ctx, query, input.Name, input.Handler, input.Description, input.State, input.Version, input.Timeout.Milliseconds(), input.Runtime, input.Image, input.MemoryMB, input.CPUMillis).Scan(
		&out.Name, &out.Handler, &out.Description, &out.State, &out.Version, &timeoutMS, &out.Runtime, &out.Image, &out.MemoryMB, &out.CPUMillis, &out.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return function.Function{}, function.ErrAlreadyExists
		}
		return function.Function{}, fmt.Errorf("create function: %w", err)
	}
	out.Timeout = time.Duration(timeoutMS) * time.Millisecond
	_, err = tx.ExecContext(ctx, `INSERT INTO function_versions (function_name, version_number, handler, runtime, image, artifact_ref, timeout_ms, memory_mb, cpu_millis, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'ACTIVE')`, out.Name, out.Version, out.Handler, out.Runtime, out.Image, out.Image, timeoutMS, out.MemoryMB, out.CPUMillis)
	if err != nil {
		return function.Function{}, fmt.Errorf("create initial version: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return function.Function{}, fmt.Errorf("commit create function: %w", err)
	}
	return out, nil
}

func (s *PostgresFunctionStore) Get(ctx context.Context, name string) (function.Function, error) {
	const query = `SELECT name, handler, description, state, active_version, timeout_ms, runtime, image, memory_mb, cpu_millis, created_at FROM functions WHERE name = $1`
	var out function.Function
	var timeoutMS int
	err := s.db.QueryRowContext(ctx, query, name).Scan(&out.Name, &out.Handler, &out.Description, &out.State, &out.Version, &timeoutMS, &out.Runtime, &out.Image, &out.MemoryMB, &out.CPUMillis, &out.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return function.Function{}, function.ErrNotFound
	}
	if err != nil {
		return function.Function{}, fmt.Errorf("get function: %w", err)
	}
	out.Timeout = time.Duration(timeoutMS) * time.Millisecond
	return out, nil
}

func (s *PostgresFunctionStore) Deploy(ctx context.Context, name string, candidate function.Function) (function.Function, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return function.Function{}, fmt.Errorf("begin deploy: %w", err)
	}
	defer tx.Rollback()
	var currentVersion int
	if err := tx.QueryRowContext(ctx, `SELECT active_version FROM functions WHERE name = $1 FOR UPDATE`, name).Scan(&currentVersion); errors.Is(err, sql.ErrNoRows) {
		return function.Function{}, function.ErrNotFound
	} else if err != nil {
		return function.Function{}, fmt.Errorf("lock function: %w", err)
	}
	candidate.Version = currentVersion + 1
	_, err = tx.ExecContext(ctx, `UPDATE function_versions SET status = 'RETIRED' WHERE function_name = $1 AND status = 'ACTIVE'`, name)
	if err != nil {
		return function.Function{}, fmt.Errorf("retire version: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO function_versions (function_name, version_number, handler, runtime, image, artifact_ref, timeout_ms, memory_mb, cpu_millis, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'ACTIVE')`, name, candidate.Version, candidate.Handler, candidate.Runtime, candidate.Image, candidate.Image, candidate.Timeout.Milliseconds(), candidate.MemoryMB, candidate.CPUMillis)
	if err != nil {
		return function.Function{}, fmt.Errorf("insert version: %w", err)
	}
	const update = `UPDATE functions SET handler=$2, description=$3, state=$4, active_version=$5, timeout_ms=$6, runtime=$7, image=$8, memory_mb=$9, cpu_millis=$10 WHERE name=$1 RETURNING name, handler, description, state, active_version, timeout_ms, runtime, image, memory_mb, cpu_millis, created_at`
	var out function.Function
	var timeoutMS int
	err = tx.QueryRowContext(ctx, update, name, candidate.Handler, candidate.Description, function.StateActive, candidate.Version, candidate.Timeout.Milliseconds(), candidate.Runtime, candidate.Image, candidate.MemoryMB, candidate.CPUMillis).Scan(&out.Name, &out.Handler, &out.Description, &out.State, &out.Version, &timeoutMS, &out.Runtime, &out.Image, &out.MemoryMB, &out.CPUMillis, &out.CreatedAt)
	if err != nil {
		return function.Function{}, fmt.Errorf("activate version: %w", err)
	}
	out.Timeout = time.Duration(timeoutMS) * time.Millisecond
	if err := tx.Commit(); err != nil {
		return function.Function{}, fmt.Errorf("commit deploy: %w", err)
	}
	return out, nil
}

func (s *PostgresFunctionStore) Rollback(ctx context.Context, name string, version int) (function.Function, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return function.Function{}, fmt.Errorf("begin rollback: %w", err)
	}
	defer tx.Rollback()
	var candidate function.Function
	var timeoutMS int
	err = tx.QueryRowContext(ctx, `SELECT handler, runtime, image, timeout_ms, memory_mb, cpu_millis FROM function_versions WHERE function_name=$1 AND version_number=$2 AND status <> 'FAILED' FOR UPDATE`, name, version).Scan(&candidate.Handler, &candidate.Runtime, &candidate.Image, &timeoutMS, &candidate.MemoryMB, &candidate.CPUMillis)
	if errors.Is(err, sql.ErrNoRows) {
		return function.Function{}, function.ErrNotFound
	}
	if err != nil {
		return function.Function{}, fmt.Errorf("read rollback version: %w", err)
	}
	candidate.Timeout = time.Duration(timeoutMS) * time.Millisecond
	candidate.Version = version
	_, err = tx.ExecContext(ctx, `UPDATE function_versions SET status='RETIRED' WHERE function_name=$1 AND status='ACTIVE'`, name)
	if err != nil {
		return function.Function{}, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE function_versions SET status='ACTIVE' WHERE function_name=$1 AND version_number=$2`, name, version)
	if err != nil {
		return function.Function{}, err
	}
	const update = `UPDATE functions SET handler=$2, runtime=$3, image=$4, timeout_ms=$5, memory_mb=$6, cpu_millis=$7, active_version=$8 WHERE name=$1 RETURNING name, handler, description, state, active_version, timeout_ms, runtime, image, memory_mb, cpu_millis, created_at`
	var out function.Function
	err = tx.QueryRowContext(ctx, update, name, candidate.Handler, candidate.Runtime, candidate.Image, candidate.Timeout.Milliseconds(), candidate.MemoryMB, candidate.CPUMillis, version).Scan(&out.Name, &out.Handler, &out.Description, &out.State, &out.Version, &timeoutMS, &out.Runtime, &out.Image, &out.MemoryMB, &out.CPUMillis, &out.CreatedAt)
	if err != nil {
		return function.Function{}, fmt.Errorf("activate rollback: %w", err)
	}
	out.Timeout = time.Duration(timeoutMS) * time.Millisecond
	if err := tx.Commit(); err != nil {
		return function.Function{}, fmt.Errorf("commit rollback: %w", err)
	}
	return out, nil
}

func isUniqueViolation(err error) bool {
	type stateError interface{ SQLState() string }
	var databaseError stateError
	return errors.As(err, &databaseError) && databaseError.SQLState() == "23505"
}
