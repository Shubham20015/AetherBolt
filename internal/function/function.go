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
	StateActive    = "ACTIVE"
	defaultTimeout = 3 * time.Second
)

var validName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

var (
	ErrNotFound       = errors.New("function not found")
	ErrAlreadyExists  = errors.New("function already exists")
	ErrUnknownHandler = errors.New("unknown built-in handler")
	ErrInvalidInput   = errors.New("invalid input")
)

type Function struct {
	Name        string        `json:"name"`
	Handler     string        `json:"handler"`
	Description string        `json:"description,omitempty"`
	State       string        `json:"state"`
	Version     int           `json:"active_version"`
	Timeout     time.Duration `json:"-"`
	CreatedAt   time.Time     `json:"created_at"`
}

type RegisterInput struct {
	Name        string `json:"name"`
	Handler     string `json:"handler"`
	Description string `json:"description"`
	TimeoutMS   int    `json:"timeout_ms"`
}

type Repository interface {
	Create(context.Context, Function) (Function, error)
	Get(context.Context, string) (Function, error)
}

type Handler interface {
	Invoke(context.Context, json.RawMessage) (json.RawMessage, error)
}

type RuntimeRegistry interface {
	Lookup(string) (Handler, bool)
}

type Service struct {
	repo     Repository
	runtimes RuntimeRegistry
}

func NewService(repo Repository, runtimes RuntimeRegistry) *Service {
	return &Service{repo: repo, runtimes: runtimes}
}

func (s *Service) Register(ctx context.Context, input RegisterInput) (Function, error) {
	input.Name = strings.ToLower(strings.TrimSpace(input.Name))
	input.Handler = strings.TrimSpace(input.Handler)
	input.Description = strings.TrimSpace(input.Description)
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
	if _, ok := s.runtimes.Lookup(input.Handler); !ok {
		return Function{}, fmt.Errorf("%w: %s", ErrUnknownHandler, input.Handler)
	}
	return s.repo.Create(ctx, Function{
		Name: input.Name, Handler: input.Handler, Description: input.Description, State: StateActive,
		Version: 1, Timeout: time.Duration(input.TimeoutMS) * time.Millisecond,
	})
}

func (s *Service) Get(ctx context.Context, name string) (Function, error) {
	return s.repo.Get(ctx, strings.ToLower(strings.TrimSpace(name)))
}

func (s *Service) Invoke(ctx context.Context, name string, event json.RawMessage) (json.RawMessage, Function, time.Duration, error) {
	if !json.Valid(event) {
		return nil, Function{}, 0, fmt.Errorf("%w: event must be valid JSON", ErrInvalidInput)
	}
	fn, err := s.Get(ctx, name)
	if err != nil {
		return nil, Function{}, 0, err
	}
	if fn.State != StateActive {
		return nil, Function{}, 0, fmt.Errorf("%w: function is not active", ErrInvalidInput)
	}
	handler, ok := s.runtimes.Lookup(fn.Handler)
	if !ok {
		return nil, Function{}, 0, fmt.Errorf("%w: %s", ErrUnknownHandler, fn.Handler)
	}
	invokeCtx, cancel := context.WithTimeout(ctx, fn.Timeout)
	defer cancel()
	started := time.Now()
	result, err := handler.Invoke(invokeCtx, event)
	return result, fn, time.Since(started), err
}
