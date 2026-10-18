package async

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/shubhamjain/aetherbolt/internal/function"
)

// fakeStore is a hook-based Store used to drive the service without a database.
type fakeStore struct {
	enqueueFn  func(context.Context, function.Function, json.RawMessage, string) (Invocation, bool, error)
	claimFn    func(context.Context, string) (Invocation, error)
	completeFn func(context.Context, Invocation, function.ExecutionResult, error, time.Duration) error
	getFn      func(context.Context, string) (Invocation, error)
	pendingFn  func(context.Context, int) ([]OutboxMessage, error)
	markFn     func(context.Context, OutboxMessage) error
}

func (s *fakeStore) Enqueue(ctx context.Context, fn function.Function, event json.RawMessage, key string) (Invocation, bool, error) {
	return s.enqueueFn(ctx, fn, event, key)
}
func (s *fakeStore) Claim(ctx context.Context, id string) (Invocation, error) {
	return s.claimFn(ctx, id)
}
func (s *fakeStore) Complete(ctx context.Context, inv Invocation, result function.ExecutionResult, runErr error, delay time.Duration) error {
	return s.completeFn(ctx, inv, result, runErr, delay)
}
func (s *fakeStore) Get(ctx context.Context, id string) (Invocation, error) {
	return s.getFn(ctx, id)
}
func (s *fakeStore) PendingOutbox(ctx context.Context, n int) ([]OutboxMessage, error) {
	return s.pendingFn(ctx, n)
}
func (s *fakeStore) MarkPublished(ctx context.Context, message OutboxMessage) error {
	return s.markFn(ctx, message)
}

type fakeExecutor struct {
	calls    int
	invokeFn func(context.Context, function.Function, json.RawMessage) (function.ExecutionResult, error)
}

func (e *fakeExecutor) Validate(function.Function) error { return nil }
func (e *fakeExecutor) Invoke(ctx context.Context, fn function.Function, event json.RawMessage) (function.ExecutionResult, error) {
	e.calls++
	if e.invokeFn == nil {
		return function.ExecutionResult{Result: event}, nil
	}
	return e.invokeFn(ctx, fn, event)
}

type publishCall struct{ id, topic string }

type fakePublisher struct {
	mu    sync.Mutex
	calls []publishCall
	fn    func(id, topic string) error
}

func (p *fakePublisher) Publish(_ context.Context, id, topic string) error {
	p.mu.Lock()
	p.calls = append(p.calls, publishCall{id: id, topic: topic})
	p.mu.Unlock()
	if p.fn != nil {
		return p.fn(id, topic)
	}
	return nil
}
func (p *fakePublisher) recorded() []publishCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]publishCall(nil), p.calls...)
}

func TestEnqueueRejectsInvalidEvent(t *testing.T) {
	called := false
	store := &fakeStore{enqueueFn: func(context.Context, function.Function, json.RawMessage, string) (Invocation, bool, error) {
		called = true
		return Invocation{}, false, nil
	}}
	service := New(store, &fakePublisher{}, &fakeExecutor{})
	if _, _, err := service.Enqueue(context.Background(), function.Function{}, json.RawMessage(`{"broken"`), ""); !errors.Is(err, function.ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
	if called {
		t.Fatal("store must not be called for an invalid event")
	}
}

func TestEnqueueDelegatesAndReportsDuplicate(t *testing.T) {
	var gotKey string
	store := &fakeStore{enqueueFn: func(_ context.Context, fn function.Function, _ json.RawMessage, key string) (Invocation, bool, error) {
		gotKey = key
		return Invocation{ID: "inv-1", FunctionName: fn.Name, Status: "QUEUED"}, true, nil
	}}
	service := New(store, &fakePublisher{}, &fakeExecutor{})
	inv, duplicate, err := service.Enqueue(context.Background(), function.Function{Name: "echo"}, json.RawMessage(`{}`), "client-key")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if gotKey != "client-key" || inv.ID != "inv-1" || !duplicate {
		t.Fatalf("unexpected enqueue result: key=%q inv=%#v duplicate=%v", gotKey, inv, duplicate)
	}
}

func TestProcessAcknowledgesUnclaimableDuplicate(t *testing.T) {
	executor := &fakeExecutor{}
	completed := false
	store := &fakeStore{
		claimFn: func(context.Context, string) (Invocation, error) { return Invocation{}, ErrNotClaimable },
		completeFn: func(context.Context, Invocation, function.ExecutionResult, error, time.Duration) error {
			completed = true
			return nil
		},
	}
	service := New(store, &fakePublisher{}, executor)
	if err := service.Process(context.Background(), "inv-1"); err != nil {
		t.Fatalf("duplicate record must be acknowledged, got %v", err)
	}
	if executor.calls != 0 || completed {
		t.Fatalf("duplicate record must not execute or complete: calls=%d completed=%v", executor.calls, completed)
	}
}

func TestProcessPropagatesClaimError(t *testing.T) {
	sentinel := errors.New("claim failed")
	store := &fakeStore{claimFn: func(context.Context, string) (Invocation, error) { return Invocation{}, sentinel }}
	executor := &fakeExecutor{}
	service := New(store, &fakePublisher{}, executor)
	if err := service.Process(context.Background(), "inv-1"); !errors.Is(err, sentinel) {
		t.Fatalf("expected claim error, got %v", err)
	}
	if executor.calls != 0 {
		t.Fatal("executor must not run when the claim fails")
	}
}

func TestProcessCompletesSuccessfulInvocation(t *testing.T) {
	result := json.RawMessage(`{"ok":true}`)
	var gotResult function.ExecutionResult
	var gotErr error
	var gotDelay time.Duration
	store := &fakeStore{
		claimFn: func(context.Context, string) (Invocation, error) {
			return Invocation{ID: "inv-1", Attempt: 1, Version: 1, Event: json.RawMessage(`{}`), Function: function.Function{Name: "echo", Timeout: time.Second}}, nil
		},
		completeFn: func(_ context.Context, _ Invocation, res function.ExecutionResult, runErr error, delay time.Duration) error {
			gotResult, gotErr, gotDelay = res, runErr, delay
			return nil
		},
	}
	executor := &fakeExecutor{invokeFn: func(context.Context, function.Function, json.RawMessage) (function.ExecutionResult, error) {
		return function.ExecutionResult{Result: result}, nil
	}}
	service := New(store, &fakePublisher{}, executor)
	if err := service.Process(context.Background(), "inv-1"); err != nil {
		t.Fatalf("process: %v", err)
	}
	if gotErr != nil || string(gotResult.Result) != string(result) {
		t.Fatalf("unexpected completion: result=%s err=%v", gotResult.Result, gotErr)
	}
	if gotDelay < 0 || gotDelay > time.Second {
		t.Fatalf("retry delay out of bounds: %v", gotDelay)
	}
}

func TestProcessMapsDeadlineToTimedOut(t *testing.T) {
	var gotErr error
	store := &fakeStore{
		claimFn: func(context.Context, string) (Invocation, error) {
			return Invocation{ID: "inv-1", Attempt: 1, Function: function.Function{Timeout: 20 * time.Millisecond}}, nil
		},
		completeFn: func(_ context.Context, _ Invocation, _ function.ExecutionResult, runErr error, _ time.Duration) error {
			gotErr = runErr
			return nil
		},
	}
	executor := &fakeExecutor{invokeFn: func(ctx context.Context, _ function.Function, _ json.RawMessage) (function.ExecutionResult, error) {
		<-ctx.Done()
		return function.ExecutionResult{}, ctx.Err()
	}}
	service := New(store, &fakePublisher{}, executor)
	if err := service.Process(context.Background(), "inv-1"); err != nil {
		t.Fatalf("process: %v", err)
	}
	if !errors.Is(gotErr, function.ErrTimedOut) {
		t.Fatalf("expected timed-out classification, got %v", gotErr)
	}
}

func TestRepublishPublishesPendingAndMarksThem(t *testing.T) {
	var mu sync.Mutex
	marked := map[int64]bool{}
	store := &fakeStore{
		pendingFn: func(context.Context, int) ([]OutboxMessage, error) {
			return []OutboxMessage{{ID: 1, InvocationID: "inv-1", Topic: "INVOCATION"}, {ID: 2, InvocationID: "inv-2", Topic: "DLQ"}}, nil
		},
		markFn: func(_ context.Context, message OutboxMessage) error {
			mu.Lock()
			defer mu.Unlock()
			marked[message.ID] = true
			return nil
		},
	}
	publisher := &fakePublisher{}
	service := New(store, publisher, &fakeExecutor{})
	if err := service.Republish(context.Background()); err != nil {
		t.Fatalf("republish: %v", err)
	}
	calls := publisher.recorded()
	if len(calls) != 2 || calls[0] != (publishCall{id: "inv-1", topic: "INVOCATION"}) || calls[1] != (publishCall{id: "inv-2", topic: "DLQ"}) {
		t.Fatalf("unexpected publish calls: %#v", calls)
	}
	if !marked[1] || !marked[2] {
		t.Fatalf("expected both records marked published: %#v", marked)
	}
}

func TestRepublishReconcilesAfterBrokerRecovery(t *testing.T) {
	var mu sync.Mutex
	marked := map[int64]bool{}
	// pendingFn models the durable outbox: only unpublished rows are returned.
	store := &fakeStore{
		pendingFn: func(context.Context, int) ([]OutboxMessage, error) {
			mu.Lock()
			defer mu.Unlock()
			var out []OutboxMessage
			for _, m := range []OutboxMessage{{ID: 1, InvocationID: "inv-1", Topic: "INVOCATION"}, {ID: 2, InvocationID: "inv-2", Topic: "INVOCATION"}} {
				if !marked[m.ID] {
					out = append(out, m)
				}
			}
			return out, nil
		},
		markFn: func(_ context.Context, message OutboxMessage) error {
			mu.Lock()
			defer mu.Unlock()
			marked[message.ID] = true
			return nil
		},
	}
	brokerDown := true
	publisher := &fakePublisher{}
	publisher.fn = func(id, topic string) error {
		if brokerDown {
			return errors.New("broker unavailable")
		}
		return nil
	}
	service := New(store, publisher, &fakeExecutor{})

	// First cycle: broker is down, so reconciliation must fail and mark nothing.
	if err := service.Republish(context.Background()); err == nil {
		t.Fatal("expected broker outage to surface as an error")
	}
	mu.Lock()
	if len(marked) != 0 {
		mu.Unlock()
		t.Fatalf("no record may be marked while the broker is down: %#v", marked)
	}
	mu.Unlock()

	// Second cycle after restart: both records are published and marked.
	brokerDown = false
	if err := service.Republish(context.Background()); err != nil {
		t.Fatalf("republish after recovery: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !marked[1] || !marked[2] {
		t.Fatalf("expected both records reconciled after recovery: %#v", marked)
	}
}

func TestRepublishPropagatesMarkError(t *testing.T) {
	sentinel := errors.New("outbox write failed")
	store := &fakeStore{
		pendingFn: func(context.Context, int) ([]OutboxMessage, error) {
			return []OutboxMessage{{ID: 1, InvocationID: "inv-1", Topic: "INVOCATION"}}, nil
		},
		markFn: func(context.Context, OutboxMessage) error { return sentinel },
	}
	service := New(store, &fakePublisher{}, &fakeExecutor{})
	if err := service.Republish(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("expected mark error, got %v", err)
	}
}

func TestRetryDelayIsBoundedAndJittered(t *testing.T) {
	caps := map[int]time.Duration{0: time.Second, 1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 6: 30 * time.Second, 100: 30 * time.Second}
	for attempt, want := range caps {
		if got := retryDelay(attempt); got < 0 || got > want {
			t.Fatalf("retryDelay(%d) = %v, want within [0,%v]", attempt, got, want)
		}
	}
	seen := map[time.Duration]struct{}{}
	for i := 0; i < 200; i++ {
		seen[retryDelay(6)] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("expected jitter to produce varied delays, got %d distinct value(s)", len(seen))
	}
}
