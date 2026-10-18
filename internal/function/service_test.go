package function_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/shubhamjain/aetherbolt/internal/function"
	"github.com/shubhamjain/aetherbolt/internal/runtime"
)

type memoryStore struct {
	mu     sync.Mutex
	values map[string]function.Function
}

func newMemoryStore() *memoryStore { return &memoryStore{values: make(map[string]function.Function)} }
func (s *memoryStore) Create(_ context.Context, fn function.Function) (function.Function, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.values[fn.Name]; exists {
		return function.Function{}, function.ErrAlreadyExists
	}
	fn.CreatedAt = time.Now().UTC()
	s.values[fn.Name] = fn
	return fn, nil
}
func (s *memoryStore) Get(_ context.Context, name string) (function.Function, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn, ok := s.values[name]
	if !ok {
		return function.Function{}, function.ErrNotFound
	}
	return fn, nil
}

func TestRegisterAndInvokeEcho(t *testing.T) {
	service := function.NewService(newMemoryStore(), runtime.NewRouter(runtime.NewDockerExecutor(runtime.NewOSCommandRunner())))
	fn, err := service.Register(context.Background(), function.RegisterInput{Name: "echo-test", Handler: "echo"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if fn.Timeout != 3*time.Second || fn.Version != 1 || fn.State != function.StateActive {
		t.Fatalf("unexpected function: %#v", fn)
	}
	execution, _, err := service.Invoke(context.Background(), "echo-test", json.RawMessage(`{"key":"value"}`))
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if string(execution.Result) != `{"key":"value"}` {
		t.Fatalf("result = %s", execution.Result)
	}
}

func TestRegisterRejectsUnknownHandlerAndDuplicateName(t *testing.T) {
	service := function.NewService(newMemoryStore(), runtime.NewRouter(runtime.NewDockerExecutor(runtime.NewOSCommandRunner())))
	_, err := service.Register(context.Background(), function.RegisterInput{Name: "unknown", Handler: "not-a-handler"})
	if !errors.Is(err, function.ErrUnknownHandler) {
		t.Fatalf("expected unknown handler error, got %v", err)
	}
	_, err = service.Register(context.Background(), function.RegisterInput{Name: "echo", Handler: "echo"})
	if err != nil {
		t.Fatalf("initial register: %v", err)
	}
	_, err = service.Register(context.Background(), function.RegisterInput{Name: "echo", Handler: "echo"})
	if !errors.Is(err, function.ErrAlreadyExists) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}
