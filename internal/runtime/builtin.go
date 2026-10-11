package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/shubhamjain/aetherbolt/internal/function"
)

type BuiltinRegistry struct{ handlers map[string]function.Handler }

func NewBuiltinRegistry() *BuiltinRegistry {
	return &BuiltinRegistry{handlers: map[string]function.Handler{
		"echo": echoHandler{}, "uppercase": uppercaseHandler{},
	}}
}

func (r *BuiltinRegistry) Lookup(name string) (function.Handler, bool) {
	h, ok := r.handlers[name]
	return h, ok
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
