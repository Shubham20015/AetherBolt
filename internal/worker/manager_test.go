package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/shubhamjain/aetherbolt/internal/function"
)

type blockingExecutor struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingExecutor() *blockingExecutor {
	return &blockingExecutor{started: make(chan struct{}), release: make(chan struct{})}
}
func (e *blockingExecutor) Validate(function.Function) error { return nil }
func (e *blockingExecutor) Invoke(ctx context.Context, _ function.Function, _ json.RawMessage) (function.ExecutionResult, error) {
	e.once.Do(func() { close(e.started) })
	select {
	case <-e.release:
		return function.ExecutionResult{Result: json.RawMessage(`{}`)}, nil
	case <-ctx.Done():
		return function.ExecutionResult{}, ctx.Err()
	}
}

func TestManagerAppliesBoundedAdmission(t *testing.T) {
	executor := newBlockingExecutor()
	manager, err := NewManager(executor, Config{MaxConcurrent: 1, QueueCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	fn := function.Function{}
	firstDone := make(chan error, 1)
	go func() { _, err := manager.Invoke(context.Background(), fn, json.RawMessage(`{}`)); firstDone <- err }()
	<-executor.started
	secondDone := make(chan error, 1)
	go func() { _, err := manager.Invoke(context.Background(), fn, json.RawMessage(`{}`)); secondDone <- err }()
	deadline := time.Now().Add(time.Second)
	for len(manager.admission) != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(manager.admission) != 2 {
		t.Fatal("second invocation was not admitted to the bounded queue")
	}
	if _, err := manager.Invoke(context.Background(), fn, json.RawMessage(`{}`)); !errors.Is(err, function.ErrCapacity) {
		t.Fatalf("expected capacity error, got %v", err)
	}
	close(executor.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first invocation: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second invocation: %v", err)
	}
}

func TestManagerDrainsWithoutAcceptingNewWork(t *testing.T) {
	executor := newBlockingExecutor()
	manager, err := NewManager(executor, Config{MaxConcurrent: 1, QueueCapacity: 0})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := manager.Invoke(context.Background(), function.Function{}, json.RawMessage(`{}`))
		done <- err
	}()
	<-executor.started
	manager.BeginDrain()
	if _, err := manager.Invoke(context.Background(), function.Function{}, json.RawMessage(`{}`)); !errors.Is(err, function.ErrDraining) {
		t.Fatalf("expected draining error, got %v", err)
	}
	close(executor.release)
	if err := <-done; err != nil {
		t.Fatalf("in-flight invocation: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Wait(ctx); err != nil {
		t.Fatalf("wait: %v", err)
	}
}
