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
	const query = `INSERT INTO functions (name, handler, description, state, active_version, timeout_ms)
	VALUES ($1, $2, $3, $4, $5, $6)
	RETURNING name, handler, description, state, active_version, timeout_ms, created_at`
	var out function.Function
	var timeoutMS int
	err := s.db.QueryRowContext(ctx, query, input.Name, input.Handler, input.Description, input.State, input.Version, input.Timeout.Milliseconds()).Scan(
		&out.Name, &out.Handler, &out.Description, &out.State, &out.Version, &timeoutMS, &out.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return function.Function{}, function.ErrAlreadyExists
		}
		return function.Function{}, fmt.Errorf("create function: %w", err)
	}
	out.Timeout = time.Duration(timeoutMS) * time.Millisecond
	return out, nil
}

func (s *PostgresFunctionStore) Get(ctx context.Context, name string) (function.Function, error) {
	const query = `SELECT name, handler, description, state, active_version, timeout_ms, created_at FROM functions WHERE name = $1`
	var out function.Function
	var timeoutMS int
	err := s.db.QueryRowContext(ctx, query, name).Scan(&out.Name, &out.Handler, &out.Description, &out.State, &out.Version, &timeoutMS, &out.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return function.Function{}, function.ErrNotFound
	}
	if err != nil {
		return function.Function{}, fmt.Errorf("get function: %w", err)
	}
	out.Timeout = time.Duration(timeoutMS) * time.Millisecond
	return out, nil
}

func isUniqueViolation(err error) bool {
	type stateError interface{ SQLState() string }
	var databaseError stateError
	return errors.As(err, &databaseError) && databaseError.SQLState() == "23505"
}
