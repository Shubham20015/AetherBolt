package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func kafkaBroker(t *testing.T) string {
	t.Helper()
	broker := os.Getenv("AETHERBOLT_TEST_KAFKA_BROKER")
	if broker == "" {
		t.Skip("set AETHERBOLT_TEST_KAFKA_BROKER to run Kafka end-to-end tests")
	}
	return broker
}

func uniqueSuffix() string {
	raw := make([]byte, 6)
	_, _ = rand.Read(raw)
	return hex.EncodeToString(raw)
}

// ensureTopics creates the topics a test needs. Creation is best-effort: the
// broker may auto-create on first publish, so an "already exists" error is fine.
func ensureTopics(t *testing.T, broker string, topics ...string) {
	t.Helper()
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial kafka: %v", err)
	}
	defer conn.Close()
	for _, topic := range topics {
		if err := conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
			t.Logf("create topic %s (may already exist): %v", topic, err)
		}
	}
}

func TestKafkaPublishConsumeRoundTripAndDLQ(t *testing.T) {
	broker := kafkaBroker(t)
	suffix := uniqueSuffix()
	topic := "aetherbolt.test." + suffix
	dlqTopic := topic + ".dlq"

	k := NewKafka([]string{broker}, topic)
	defer k.Close()
	ensureTopics(t, broker, topic, dlqTopic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	received := make(chan string, 1)
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- k.Consume(ctx, "aetherbolt-test-"+suffix, func(_ context.Context, id string) error {
			select {
			case received <- id:
			default:
			}
			return nil
		})
	}()

	if err := k.Publish(ctx, "inv-round-trip", "INVOCATION"); err != nil {
		t.Fatalf("publish invocation: %v", err)
	}

	select {
	case id := <-received:
		if id != "inv-round-trip" {
			t.Fatalf("received %q, want inv-round-trip", id)
		}
	case err := <-consumeErr:
		t.Fatalf("consumer stopped early: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the round-trip message")
	}

	// DLQ routing must publish to the derived "<topic>.dlq" topic.
	if err := k.Publish(ctx, "inv-dead-letter", "DLQ"); err != nil {
		t.Fatalf("publish dlq: %v", err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker}, Topic: dlqTopic, Partition: 0,
		MinBytes: 1, MaxBytes: 1 << 20, MaxWait: time.Second, StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()
	readCtx, readCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer readCancel()
	message, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("read dlq message: %v", err)
	}
	if !strings.Contains(string(message.Value), "inv-dead-letter") {
		t.Fatalf("dlq message = %s, want it to reference inv-dead-letter", message.Value)
	}

	// Cancelling the context must stop the consumer goroutine cleanly.
	cancel()
	select {
	case err := <-consumeErr:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("consumer returned unexpected error on shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop after cancellation")
	}
}
