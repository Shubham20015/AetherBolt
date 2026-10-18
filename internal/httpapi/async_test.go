package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	asyncsvc "github.com/shubhamjain/aetherbolt/internal/async"
	"github.com/shubhamjain/aetherbolt/internal/function"
	"github.com/shubhamjain/aetherbolt/internal/runtime"
)

type fakeAsyncStore struct {
	enqueueFn func(context.Context, function.Function, json.RawMessage, string) (asyncsvc.Invocation, bool, error)
	getFn     func(context.Context, string) (asyncsvc.Invocation, error)
}

func (s *fakeAsyncStore) Enqueue(ctx context.Context, fn function.Function, event json.RawMessage, key string) (asyncsvc.Invocation, bool, error) {
	return s.enqueueFn(ctx, fn, event, key)
}
func (s *fakeAsyncStore) Claim(context.Context, string) (asyncsvc.Invocation, error) {
	return asyncsvc.Invocation{}, errors.New("unused")
}
func (s *fakeAsyncStore) Complete(context.Context, asyncsvc.Invocation, function.ExecutionResult, error, time.Duration) error {
	return errors.New("unused")
}
func (s *fakeAsyncStore) Get(ctx context.Context, id string) (asyncsvc.Invocation, error) {
	return s.getFn(ctx, id)
}
func (s *fakeAsyncStore) PendingOutbox(context.Context, int) ([]asyncsvc.OutboxMessage, error) {
	return nil, errors.New("unused")
}
func (s *fakeAsyncStore) MarkPublished(context.Context, asyncsvc.OutboxMessage) error {
	return errors.New("unused")
}

type fakePublisher struct{}

func (fakePublisher) Publish(context.Context, string, string) error { return nil }

type fakeExecutor struct{}

func (fakeExecutor) Validate(function.Function) error { return nil }
func (fakeExecutor) Invoke(context.Context, function.Function, json.RawMessage) (function.ExecutionResult, error) {
	return function.ExecutionResult{}, errors.New("unused")
}

func asyncHandler(t *testing.T, store *fakeAsyncStore) http.Handler {
	t.Helper()
	repo := newTestStore()
	if _, err := repo.Create(context.Background(), function.Function{
		Name: "upper", Handler: "uppercase", State: function.StateActive, Version: 1,
		Timeout: time.Second, Runtime: function.RuntimeBuiltin, MemoryMB: 128, CPUMillis: 250,
	}); err != nil {
		t.Fatalf("seed function: %v", err)
	}
	service := function.NewService(repo, runtime.NewRouter(runtime.NewDockerExecutor(runtime.NewOSCommandRunner())))
	return NewHandler(service, slog.New(slog.NewTextHandler(testingWriter{t}, nil))).WithAsync(asyncsvc.New(store, fakePublisher{}, fakeExecutor{}))
}

func TestHTTPAsyncInvokeAndStatus(t *testing.T) {
	server := httptest.NewServer(asyncHandler(t, &fakeAsyncStore{
		enqueueFn: func(_ context.Context, fn function.Function, _ json.RawMessage, _ string) (asyncsvc.Invocation, bool, error) {
			return asyncsvc.Invocation{ID: "inv-1", FunctionName: fn.Name, Version: fn.Version, Status: "QUEUED"}, false, nil
		},
		getFn: func(_ context.Context, id string) (asyncsvc.Invocation, error) {
			return asyncsvc.Invocation{ID: id, FunctionName: "upper", Version: 1, Status: "SUCCEEDED", Attempt: 1, MaxAttempts: 3, Result: json.RawMessage(`{"text":"HELLO"}`)}, nil
		},
	}))
	defer server.Close()

	accepted := request(t, http.MethodPost, server.URL+"/v1/functions/upper:invoke-async", `{"event":{"text":"hello"}}`)
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("invoke-async status = %d, want 202", accepted.StatusCode)
	}
	var acceptance struct {
		InvocationID string `json:"invocation_id"`
		Status       string `json:"status"`
		Duplicate    bool   `json:"duplicate"`
	}
	if err := json.NewDecoder(accepted.Body).Decode(&acceptance); err != nil {
		t.Fatal(err)
	}
	if acceptance.InvocationID != "inv-1" || acceptance.Status != "QUEUED" || acceptance.Duplicate {
		t.Fatalf("unexpected acceptance: %#v", acceptance)
	}

	status := request(t, http.MethodGet, server.URL+"/v1/invocations/inv-1", "")
	if status.StatusCode != http.StatusOK {
		t.Fatalf("invocation status = %d, want 200", status.StatusCode)
	}
	var statusBody struct {
		Status   string `json:"status"`
		Attempts int    `json:"attempt_count"`
	}
	if err := json.NewDecoder(status.Body).Decode(&statusBody); err != nil {
		t.Fatal(err)
	}
	if statusBody.Status != "SUCCEEDED" || statusBody.Attempts != 1 {
		t.Fatalf("unexpected status body: %#v", statusBody)
	}
}

func TestHTTPIdempotencyConflictReturns409(t *testing.T) {
	server := httptest.NewServer(asyncHandler(t, &fakeAsyncStore{
		enqueueFn: func(context.Context, function.Function, json.RawMessage, string) (asyncsvc.Invocation, bool, error) {
			return asyncsvc.Invocation{}, false, fmt.Errorf("%w: idempotency key has a different payload", function.ErrIdempotencyConflict)
		},
		getFn: func(context.Context, string) (asyncsvc.Invocation, error) { return asyncsvc.Invocation{}, function.ErrNotFound },
	}))
	defer server.Close()

	response := request(t, http.MethodPost, server.URL+"/v1/functions/upper:invoke-async", `{"event":{"text":"hello"},"idempotency_key":"k"}`)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", response.StatusCode)
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "idempotency_conflict" {
		t.Fatalf("error code = %q, want idempotency_conflict", body.Error.Code)
	}
}

func TestHTTPAsyncInvokeRequiresValidEvent(t *testing.T) {
	server := httptest.NewServer(asyncHandler(t, &fakeAsyncStore{
		enqueueFn: func(context.Context, function.Function, json.RawMessage, string) (asyncsvc.Invocation, bool, error) {
			t.Fatal("store must not be called when the event is missing")
			return asyncsvc.Invocation{}, false, nil
		},
		getFn: func(context.Context, string) (asyncsvc.Invocation, error) { return asyncsvc.Invocation{}, function.ErrNotFound },
	}))
	defer server.Close()

	response := request(t, http.MethodPost, server.URL+"/v1/functions/upper:invoke-async", `{}`)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.StatusCode)
	}
}

func TestHTTPAsyncRoutesUnavailableWithoutService(t *testing.T) {
	service := function.NewService(newTestStore(), runtime.NewRouter(runtime.NewDockerExecutor(runtime.NewOSCommandRunner())))
	server := httptest.NewServer(NewHandler(service, slog.New(slog.NewTextHandler(testingWriter{t}, nil))))
	defer server.Close()

	for _, path := range []string{"/v1/invocations/anything"} {
		response := request(t, http.MethodGet, server.URL+path, "")
		if response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("GET %s status = %d, want 503", path, response.StatusCode)
		}
	}
}
