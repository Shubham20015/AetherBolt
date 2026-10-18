package function

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	StateActive      = "ACTIVE"
	RuntimeBuiltin   = "builtin"
	RuntimeContainer = "container"
	defaultTimeout   = 3 * time.Second
	defaultMemoryMB  = 128
	defaultCPUMillis = 250
)

var validName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

var (
	ErrNotFound       = errors.New("function not found")
	ErrAlreadyExists  = errors.New("function already exists")
	ErrUnknownHandler = errors.New("unknown built-in handler")
	ErrInvalidInput   = errors.New("invalid input")
	ErrCapacity       = errors.New("execution capacity exhausted")
	ErrDraining       = errors.New("execution scheduler is draining")
	ErrTimedOut       = errors.New("function execution timed out")
	ErrFunctionFailed = errors.New("function execution failed")
	ErrRuntimeFailure = errors.New("execution runtime failed")
)

type Function struct {
	Name        string        `json:"name"`
	Handler     string        `json:"handler"`
	Description string        `json:"description,omitempty"`
	State       string        `json:"state"`
	Version     int           `json:"active_version"`
	Timeout     time.Duration `json:"-"`
	Runtime     string        `json:"runtime"`
	Image       string        `json:"image,omitempty"`
	MemoryMB    int           `json:"memory_mb"`
	CPUMillis   int           `json:"cpu_millis"`
	CreatedAt   time.Time     `json:"created_at"`
}

type RegisterInput struct {
	Name        string `json:"name"`
	Handler     string `json:"handler"`
	Description string `json:"description"`
	TimeoutMS   int    `json:"timeout_ms"`
	Runtime     string `json:"runtime"`
	Image       string `json:"image"`
	MemoryMB    int    `json:"memory_mb"`
	CPUMillis   int    `json:"cpu_millis"`
}

type Repository interface {
	Create(context.Context, Function) (Function, error)
	Get(context.Context, string) (Function, error)
}

type ExecutionResult struct {
	Result          json.RawMessage
	InitDuration    time.Duration
	HandlerDuration time.Duration
	ColdStart       bool
}

type Executor interface {
	Validate(Function) error
	Invoke(context.Context, Function, json.RawMessage) (ExecutionResult, error)
}

type Service struct {
	repo     Repository
	executor Executor
}

func NewService(repo Repository, executor Executor) *Service {
	return &Service{repo: repo, executor: executor}
}

func (s *Service) Register(ctx context.Context, input RegisterInput) (Function, error) {
	input.Name = strings.ToLower(strings.TrimSpace(input.Name))
	input.Handler = strings.TrimSpace(input.Handler)
	input.Description = strings.TrimSpace(input.Description)
	input.Runtime = strings.ToLower(strings.TrimSpace(input.Runtime))
	input.Image = strings.TrimSpace(input.Image)
	if !validName.MatchString(input.Name) {
		return Function{}, fmt.Errorf("%w: name must match %s", ErrInvalidInput, validName.String())
	}
	if input.Handler == "" {
		return Function{}, fmt.Errorf("%w: handler is required", ErrInvalidInput)
	}
	if len(input.Description) > 500 {
		return Function{}, fmt.Errorf("%w: description exceeds 500 characters", ErrInvalidInput)
	}
	if input.TimeoutMS == 0 {
		input.TimeoutMS = int(defaultTimeout / time.Millisecond)
	}
	if input.TimeoutMS < 1 || input.TimeoutMS > 30_000 {
		return Function{}, fmt.Errorf("%w: timeout_ms must be between 1 and 30000", ErrInvalidInput)
	}
	if input.Runtime == "" {
		input.Runtime = RuntimeBuiltin
	}
	if input.MemoryMB == 0 {
		input.MemoryMB = defaultMemoryMB
	}
	if input.CPUMillis == 0 {
		input.CPUMillis = defaultCPUMillis
	}
	if input.MemoryMB < 64 || input.MemoryMB > 1024 {
		return Function{}, fmt.Errorf("%w: memory_mb must be between 64 and 1024", ErrInvalidInput)
	}
	if input.CPUMillis < 50 || input.CPUMillis > 1000 {
		return Function{}, fmt.Errorf("%w: cpu_millis must be between 50 and 1000", ErrInvalidInput)
	}
	fn := Function{
		Name: input.Name, Handler: input.Handler, Description: input.Description, State: StateActive,
		Version: 1, Timeout: time.Duration(input.TimeoutMS) * time.Millisecond, Runtime: input.Runtime,
		Image: input.Image, MemoryMB: input.MemoryMB, CPUMillis: input.CPUMillis,
	}
	if err := s.executor.Validate(fn); err != nil {
		return Function{}, err
	}
	return s.repo.Create(ctx, fn)
}

func (s *Service) Get(ctx context.Context, name string) (Function, error) {
	return s.repo.Get(ctx, strings.ToLower(strings.TrimSpace(name)))
}

func (s *Service) Invoke(ctx context.Context, name string, event json.RawMessage) (ExecutionResult, Function, error) {
	if !json.Valid(event) {
		return ExecutionResult{}, Function{}, fmt.Errorf("%w: event must be valid JSON", ErrInvalidInput)
	}
	fn, err := s.Get(ctx, name)
	if err != nil {
		return ExecutionResult{}, Function{}, err
	}
	if fn.State != StateActive {
		return ExecutionResult{}, Function{}, fmt.Errorf("%w: function is not active", ErrInvalidInput)
	}
	invokeCtx, cancel := context.WithTimeout(ctx, fn.Timeout)
	defer cancel()
	result, err := s.executor.Invoke(invokeCtx, fn, event)
	if errors.Is(invokeCtx.Err(), context.DeadlineExceeded) {
		return ExecutionResult{}, fn, fmt.Errorf("%w: %v", ErrTimedOut, invokeCtx.Err())
	}
	return result, fn, err
}
