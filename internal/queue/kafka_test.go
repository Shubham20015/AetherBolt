package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConsumerBackoffIsBoundedAndMonotonic(t *testing.T) {
	if got := consumerBackoff(0); got != consumerRetryBase {
		t.Fatalf("first backoff = %v, want %v", got, consumerRetryBase)
	}
	previous := time.Duration(0)
	for attempt := 0; attempt < 12; attempt++ {
		d := consumerBackoff(attempt)
		if d <= 0 || d > consumerRetryMax {
			t.Fatalf("consumerBackoff(%d) = %v, want within (0,%v]", attempt, d, consumerRetryMax)
		}
		if d < previous {
			t.Fatalf("backoff must not decrease: %v then %v", previous, d)
		}
		previous = d
	}
}

func TestRetryHandlerRetriesThenSucceeds(t *testing.T) {
	attempts := 0
	err := retryHandler(context.Background(), func(context.Context, string) error {
		attempts++
		if attempts < 3 {
			return errors.New("transient")
		}
		return nil
	}, "inv-1")
	if err != nil {
		t.Fatalf("retryHandler: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestRetryHandlerStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := retryHandler(ctx, func(context.Context, string) error { return errors.New("permanent") }, "inv-1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}
