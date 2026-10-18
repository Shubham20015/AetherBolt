package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shubhamjain/aetherbolt/internal/function"
	"github.com/shubhamjain/aetherbolt/internal/runtime"
)

type testStore struct {
	mu     sync.Mutex
	values map[string]function.Function
}

func newTestStore() *testStore { return &testStore{values: map[string]function.Function{}} }
func (s *testStore) Create(_ context.Context, fn function.Function) (function.Function, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[fn.Name]; ok {
		return function.Function{}, function.ErrAlreadyExists
	}
	fn.CreatedAt = time.Now().UTC()
	s.values[fn.Name] = fn
	return fn, nil
}
func (s *testStore) Get(_ context.Context, name string) (function.Function, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.values[name]
	if !ok {
		return function.Function{}, function.ErrNotFound
	}
	return f, nil
}

func TestHTTPRegisterAndInvoke(t *testing.T) {
	service := function.NewService(newTestStore(), runtime.NewRouter(runtime.NewDockerExecutor(runtime.NewOSCommandRunner())))
	server := httptest.NewServer(NewHandler(service, slog.New(slog.NewTextHandler(testingWriter{t}, nil))))
	defer server.Close()
	register := request(t, http.MethodPost, server.URL+"/v1/functions", `{"name":"upper","handler":"uppercase","timeout_ms":1000}`)
	if register.StatusCode != http.StatusCreated {
		t.Fatalf("register status = %d", register.StatusCode)
	}
	if got := register.Header.Get("X-Request-ID"); got == "" {
		t.Fatal("missing request ID")
	}
	invoke := request(t, http.MethodPost, server.URL+"/v1/functions/invoke?name=upper", `{"event":{"text":"hello"}}`)
	if invoke.StatusCode != http.StatusOK {
		t.Fatalf("invoke status = %d", invoke.StatusCode)
	}
	var body struct {
		Result struct {
			Text string `json:"text"`
		} `json:"result"`
	}
	if err := json.NewDecoder(invoke.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Result.Text != "HELLO" {
		t.Fatalf("result = %q", body.Result.Text)
	}
}

func TestHTTPReturnsStructuredNotFound(t *testing.T) {
	service := function.NewService(newTestStore(), runtime.NewRouter(runtime.NewDockerExecutor(runtime.NewOSCommandRunner())))
	server := httptest.NewServer(NewHandler(service, slog.New(slog.NewTextHandler(testingWriter{t}, nil))))
	defer server.Close()
	response := request(t, http.MethodGet, server.URL+"/v1/functions?name=missing", "")
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var body struct {
		Error struct {
			Code      string `json:"code"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "function_not_found" || body.Error.RequestID == "" {
		t.Fatalf("unexpected error: %#v", body.Error)
	}
}

func TestHTTPRequiresNameQueryParameter(t *testing.T) {
	service := function.NewService(newTestStore(), runtime.NewRouter(runtime.NewDockerExecutor(runtime.NewOSCommandRunner())))
	server := httptest.NewServer(NewHandler(service, slog.New(slog.NewTextHandler(testingWriter{t}, nil))))
	defer server.Close()

	cases := []struct{ method, path, body string }{
		{http.MethodGet, "/v1/functions", ""},
		{http.MethodPost, "/v1/functions/invoke", `{"event":{"text":"hi"}}`},
		{http.MethodPost, "/v1/functions/versions", `{"handler":"uppercase"}`},
		{http.MethodPost, "/v1/functions/rollback", `{"version":1}`},
	}
	for _, tc := range cases {
		response := request(t, tc.method, server.URL+tc.path, tc.body)
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s %s status = %d, want 400", tc.method, tc.path, response.StatusCode)
		}
		var body struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Error.Code != "invalid_request" {
			t.Fatalf("%s %s error code = %q, want invalid_request", tc.method, tc.path, body.Error.Code)
		}
	}
}

func request(t *testing.T, method, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

type testingWriter struct{ t *testing.T }

func (w testingWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}
