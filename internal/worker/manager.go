package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/shubhamjain/aetherbolt/internal/function"
)

type Config struct {
	MaxConcurrent int
	QueueCapacity int
}

type Manager struct {
	executor  function.Executor
	admission chan struct{}
	slots     chan struct{}

	mu        sync.Mutex
	accepting bool
	inFlight  sync.WaitGroup
}

func NewManager(executor function.Executor, config Config) (*Manager, error) {
	if config.MaxConcurrent < 1 {
		return nil, fmt.Errorf("max concurrent must be positive")
	}
	if config.QueueCapacity < 0 {
		return nil, fmt.Errorf("queue capacity cannot be negative")
	}
	return &Manager{
		executor: executor, admission: make(chan struct{}, config.MaxConcurrent+config.QueueCapacity),
		slots: make(chan struct{}, config.MaxConcurrent), accepting: true,
	}, nil
}

func (m *Manager) Validate(fn function.Function) error { return m.executor.Validate(fn) }

func (m *Manager) Invoke(ctx context.Context, fn function.Function, event json.RawMessage) (function.ExecutionResult, error) {
	m.mu.Lock()
	if !m.accepting {
		m.mu.Unlock()
		return function.ExecutionResult{}, function.ErrDraining
	}
	m.inFlight.Add(1)
	m.mu.Unlock()
	defer m.inFlight.Done()

	select {
	case m.admission <- struct{}{}:
		defer func() { <-m.admission }()
	default:
		return function.ExecutionResult{}, function.ErrCapacity
	}

	select {
	case m.slots <- struct{}{}:
		defer func() { <-m.slots }()
	case <-ctx.Done():
		return function.ExecutionResult{}, ctx.Err()
	}
	return m.executor.Invoke(ctx, fn, event)
}

func (m *Manager) BeginDrain() {
	m.mu.Lock()
	m.accepting = false
	m.mu.Unlock()
}

func (m *Manager) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() { m.inFlight.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
