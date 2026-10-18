package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	asyncsvc "github.com/shubhamjain/aetherbolt/internal/async"
	"github.com/shubhamjain/aetherbolt/internal/function"
)

const maxBodyBytes = 1 << 20

type Handler struct {
	functions *function.Service
	logger    *slog.Logger
	async     *asyncsvc.Service
}

func (h *Handler) WithAsync(service *asyncsvc.Service) *Handler { h.async = service; return h }

func NewHandler(functions *function.Service, logger *slog.Logger) *Handler {
	return &Handler{functions: functions, logger: logger}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestIDMiddleware(http.HandlerFunc(h.serveHTTP)).ServeHTTP(w, r)
}

func (h *Handler) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/healthz":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/functions":
		h.register(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/functions":
		h.get(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/functions/versions":
		h.deploy(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/functions/rollback":
		h.rollback(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/functions/invoke":
		h.invoke(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/functions/invoke-async":
		h.invokeAsync(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/invocations/"):
		h.getInvocation(w, r)
	default:
		writeError(w, r, http.StatusNotFound, "not_found", "route not found")
	}
}
func (h *Handler) getInvocation(w http.ResponseWriter, r *http.Request) {
	if h.async == nil {
		writeError(w, r, http.StatusServiceUnavailable, "async_unavailable", "async invocation is not configured")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/invocations/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, r, http.StatusNotFound, "not_found", "route not found")
		return
	}
	inv, err := h.async.Get(r.Context(), id)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"invocation_id": inv.ID, "function": inv.FunctionName, "version": inv.Version, "status": inv.Status, "attempt_count": inv.Attempt, "max_attempts": inv.MaxAttempts, "next_attempt_at": inv.NextAttemptAt, "result": inv.Result, "error_code": inv.ErrorCode})
}
func (h *Handler) invokeAsync(w http.ResponseWriter, r *http.Request) {
	if h.async == nil {
		writeError(w, r, http.StatusServiceUnavailable, "async_unavailable", "async invocation is not configured")
		return
	}
	name, ok := queryName(w, r)
	if !ok {
		return
	}
	var in struct {
		Event          json.RawMessage `json:"event"`
		IdempotencyKey string          `json:"idempotency_key"`
	}
	if !decodeJSON(w, r, &in) {
		return
	}
	fn, err := h.functions.Get(r.Context(), name)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	inv, duplicate, err := h.async.Enqueue(r.Context(), fn, in.Event, in.IdempotencyKey)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"invocation_id": inv.ID, "status": inv.Status, "duplicate": duplicate})
}
func (h *Handler) deploy(w http.ResponseWriter, r *http.Request) {
	name, ok := queryName(w, r)
	if !ok {
		return
	}
	var input function.RegisterInput
	if !decodeJSON(w, r, &input) {
		return
	}
	fn, err := h.functions.Deploy(r.Context(), name, input)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, responseFunction(fn))
}
func (h *Handler) rollback(w http.ResponseWriter, r *http.Request) {
	name, ok := queryName(w, r)
	if !ok {
		return
	}
	var input struct {
		Version int `json:"version"`
	}
	if !decodeJSON(w, r, &input) {
		return
	}
	if input.Version < 1 {
		writeError(w, r, http.StatusBadRequest, "invalid_request", "version must be positive")
		return
	}
	fn, err := h.functions.Rollback(r.Context(), name, input.Version)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, responseFunction(fn))
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	var input function.RegisterInput
	if !decodeJSON(w, r, &input) {
		return
	}
	fn, err := h.functions.Register(r.Context(), input)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, responseFunction(fn))
}
func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	name, ok := queryName(w, r)
	if !ok {
		return
	}
	fn, err := h.functions.Get(r.Context(), name)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, responseFunction(fn))
}
func (h *Handler) invoke(w http.ResponseWriter, r *http.Request) {
	name, ok := queryName(w, r)
	if !ok {
		return
	}
	var input struct {
		Event json.RawMessage `json:"event"`
	}
	if !decodeJSON(w, r, &input) {
		return
	}
	if len(input.Event) == 0 {
		writeError(w, r, http.StatusBadRequest, "invalid_request", "event is required")
		return
	}
	execution, fn, err := h.functions.Invoke(r.Context(), name, input.Event)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"invocation_id": requestID(r), "function": fn.Name, "version": fn.Version, "result": execution.Result, "init_duration_ms": execution.InitDuration.Milliseconds(), "handler_duration_ms": execution.HandlerDuration.Milliseconds(), "cold_start": execution.ColdStart})
}
func (h *Handler) writeDomainError(w http.ResponseWriter, r *http.Request, err error) {
	h.logger.Warn("request failed", "request_id", requestID(r), "error", err)
	switch {
	case errors.Is(err, function.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "function_not_found", err.Error())
	case errors.Is(err, function.ErrAlreadyExists):
		writeError(w, r, http.StatusConflict, "function_already_exists", err.Error())
	case errors.Is(err, function.ErrIdempotencyConflict):
		writeError(w, r, http.StatusConflict, "idempotency_conflict", err.Error())
	case errors.Is(err, function.ErrUnknownHandler), errors.Is(err, function.ErrInvalidInput):
		writeError(w, r, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, function.ErrCapacity):
		writeError(w, r, http.StatusTooManyRequests, "capacity_exhausted", "execution capacity is currently exhausted")
	case errors.Is(err, function.ErrDraining):
		writeError(w, r, http.StatusServiceUnavailable, "service_draining", "service is shutting down")
	case errors.Is(err, function.ErrTimedOut), errors.Is(err, context.DeadlineExceeded):
		writeError(w, r, http.StatusGatewayTimeout, "invocation_timed_out", "function execution exceeded its deadline")
	case errors.Is(err, function.ErrFunctionFailed):
		writeError(w, r, http.StatusUnprocessableEntity, "function_error", "function execution failed")
	case errors.Is(err, function.ErrRuntimeFailure):
		writeError(w, r, http.StatusBadGateway, "runtime_unavailable", "execution runtime is unavailable")
	default:
		writeError(w, r, http.StatusInternalServerError, "internal_error", "unexpected server error")
	}
}

// queryName reads the required function name from the ?name= query parameter.
func queryName(w http.ResponseWriter, r *http.Request) (string, bool) {
	name := r.URL.Query().Get("name")
	if name == "" {
		writeError(w, r, http.StatusBadRequest, "invalid_request", "name query parameter is required")
		return "", false
	}
	return name, true
}
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, r, http.StatusBadRequest, "invalid_json", "request body must contain one JSON object")
		return false
	}
	return true
}
func responseFunction(fn function.Function) map[string]any {
	return map[string]any{"name": fn.Name, "handler": fn.Handler, "description": fn.Description, "state": fn.State, "active_version": fn.Version, "timeout_ms": fn.Timeout.Milliseconds(), "runtime": fn.Runtime, "image": fn.Image, "memory_mb": fn.MemoryMB, "cpu_millis": fn.CPUMillis, "created_at": fn.CreatedAt}
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message, "request_id": requestID(r)}})
}

type requestIDKey struct{}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if len(id) < 8 || len(id) > 128 {
			id = newRequestID()
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
	})
}
func requestID(r *http.Request) string {
	id, _ := r.Context().Value(requestIDKey{}).(string)
	return id
}
func newRequestID() string {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "fallback-" + time.Now().UTC().Format("20060102150405.000000000")
	}
	return hex.EncodeToString(bytes)
}
