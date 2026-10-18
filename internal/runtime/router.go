package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/shubhamjain/aetherbolt/internal/function"
)

type handler interface {
	Invoke(context.Context, json.RawMessage) (json.RawMessage, error)
}

type Router struct {
	builtins   map[string]handler
	containers *DockerExecutor
}

func NewRouter(containers *DockerExecutor) *Router {
	return &Router{builtins: map[string]handler{"echo": echoHandler{}, "uppercase": uppercaseHandler{}}, containers: containers}
}

func (r *Router) Validate(fn function.Function) error {
	switch fn.Runtime {
	case function.RuntimeBuiltin:
		if _, ok := r.builtins[fn.Handler]; !ok {
			return fmt.Errorf("%w: %s", function.ErrUnknownHandler, fn.Handler)
		}
		if fn.Image != "" {
			return fmt.Errorf("%w: built-in functions cannot set image", function.ErrInvalidInput)
		}
		return nil
	case function.RuntimeContainer:
		return r.containers.Validate(fn)
	default:
		return fmt.Errorf("%w: runtime must be builtin or container", function.ErrInvalidInput)
	}
}

func (r *Router) Invoke(ctx context.Context, fn function.Function, event json.RawMessage) (function.ExecutionResult, error) {
	if fn.Runtime == function.RuntimeContainer {
		return r.containers.Invoke(ctx, fn, event)
	}
	h, ok := r.builtins[fn.Handler]
	if !ok {
		return function.ExecutionResult{}, fmt.Errorf("%w: %s", function.ErrUnknownHandler, fn.Handler)
	}
	started := time.Now()
	result, err := h.Invoke(ctx, event)
	if err != nil {
		return function.ExecutionResult{}, fmt.Errorf("%w: %v", function.ErrFunctionFailed, err)
	}
	elapsed := time.Since(started)
	return function.ExecutionResult{Result: result, HandlerDuration: elapsed}, nil
}

type echoHandler struct{}

func (echoHandler) Invoke(ctx context.Context, event json.RawMessage) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), event...), nil
}

type uppercaseHandler struct{}

func (uppercaseHandler) Invoke(ctx context.Context, event json.RawMessage) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var input struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(event, &input); err != nil {
		return nil, fmt.Errorf("decode event: %w", err)
	}
	if input.Text == "" {
		return nil, fmt.Errorf("event.text is required")
	}
	return json.Marshal(map[string]string{"text": strings.ToUpper(input.Text)})
}
