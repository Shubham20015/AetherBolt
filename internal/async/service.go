package async

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/shubhamjain/aetherbolt/internal/function"
)

type Invocation struct {
	ID, FunctionName, Status      string
	Version, Attempt, MaxAttempts int
	Event                         json.RawMessage
	Function                      function.Function
	Result                        json.RawMessage
	ErrorCode                     string
	NextAttemptAt                 time.Time
}
type OutboxMessage struct {
	ID                  int64
	InvocationID, Topic string
}
type Store interface {
	Enqueue(context.Context, function.Function, json.RawMessage, string) (Invocation, bool, error)
	Claim(context.Context, string) (Invocation, error)
	Complete(context.Context, Invocation, function.ExecutionResult, error, time.Duration) error
	Get(context.Context, string) (Invocation, error)
	PendingOutbox(context.Context, int) ([]OutboxMessage, error)
	MarkPublished(context.Context, OutboxMessage) error
	// ReclaimExpired resets invocations whose worker lease elapsed and returns
	// how many records were recovered.
	ReclaimExpired(context.Context) (int, error)
}

// completionTimeout bounds the terminal write so a stalled database cannot
// block shutdown indefinitely while still letting the write survive
// cancellation of the consumer context.
const completionTimeout = 5 * time.Second

type Publisher interface {
	Publish(context.Context, string, string) error
}
type Service struct {
	store     Store
	publisher Publisher
	executor  function.Executor
}

func New(store Store, publisher Publisher, executor function.Executor) *Service {
	return &Service{store, publisher, executor}
}
func (s *Service) Enqueue(ctx context.Context, fn function.Function, event json.RawMessage, key string) (Invocation, bool, error) {
	if !json.Valid(event) {
		return Invocation{}, false, fmt.Errorf("%w: event must be valid JSON", function.ErrInvalidInput)
	}
	// Parity with the synchronous path: only an ACTIVE function may accept work.
	if fn.State != function.StateActive {
		return Invocation{}, false, fmt.Errorf("%w: function is not active", function.ErrInvalidInput)
	}
	inv, duplicate, err := s.store.Enqueue(ctx, fn, event, key)
	if err != nil {
		return Invocation{}, false, err
	}
	// The republisher is the only publisher.  Keeping this path transactional
	// avoids acknowledging a broker write that cannot be recorded in Postgres.
	return inv, duplicate, nil
}
func (s *Service) Process(ctx context.Context, id string) error {
	inv, err := s.store.Claim(ctx, id)
	if errors.Is(err, ErrNotClaimable) {
		// A duplicate Kafka record is expected with at-least-once delivery.
		return nil
	}
	if err != nil {
		return err
	}
	invokeCtx, cancel := context.WithTimeout(ctx, inv.Function.Timeout)
	defer cancel()
	result, runErr := s.executor.Invoke(invokeCtx, inv.Function, inv.Event)
	if errors.Is(invokeCtx.Err(), context.DeadlineExceeded) {
		runErr = function.ErrTimedOut
	}
	// The terminal write must survive shutdown cancellation of the consumer
	// context, otherwise the invocation is stranded in RUNNING until (or
	// unless) lease reclaim recovers it.  It remains bounded so a stalled
	// database cannot block shutdown forever.
	completeCtx, completeCancel := context.WithTimeout(context.WithoutCancel(ctx), completionTimeout)
	defer completeCancel()
	return s.store.Complete(completeCtx, inv, result, runErr, retryDelay(inv.Attempt))
}
func (s *Service) Reconcile(ctx context.Context) error {
	_, err := s.store.ReclaimExpired(ctx)
	return err
}
func (s *Service) Republish(ctx context.Context) error {
	ids, err := s.store.PendingOutbox(ctx, 100)
	if err != nil {
		return err
	}
	for _, message := range ids {
		if err := s.publisher.Publish(ctx, message.InvocationID, message.Topic); err != nil {
			return err
		}
		if err := s.store.MarkPublished(ctx, message); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) Get(ctx context.Context, id string) (Invocation, error) {
	return s.store.Get(ctx, id)
}

var ErrNotClaimable = errors.New("invocation not claimable")

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Second * time.Duration(1<<(attempt-1))
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	// Full jitter prevents synchronized retry storms when many invocations fail together.
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return d / 2
	}
	return time.Duration(binary.BigEndian.Uint64(random[:]) % uint64(d+1))
}
func NewID() string { b := make([]byte, 16); _, _ = rand.Read(b); return hex.EncodeToString(b) }
func Hash(event json.RawMessage) string {
	sum := sha256.Sum256(event)
	return hex.EncodeToString(sum[:])
}
