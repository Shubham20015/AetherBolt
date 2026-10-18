package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	brokers  []string
	topic    string
	dlqTopic string
	writer   *kafka.Writer
}

func NewKafka(brokers []string, topic string) *Kafka {
	return &Kafka{brokers: brokers, topic: topic, dlqTopic: topic + ".dlq", writer: &kafka.Writer{Addr: kafka.TCP(brokers...), RequiredAcks: kafka.RequireAll, Async: false}}
}
func (k *Kafka) Publish(ctx context.Context, id, topic string) error {
	b, _ := json.Marshal(map[string]string{"invocation_id": id})
	if topic == "DLQ" {
		topic = k.dlqTopic
	} else {
		topic = k.topic
	}
	return k.writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(id), Value: b})
}
func (k *Kafka) Close() error { return k.writer.Close() }

const (
	consumerRetryBase = 200 * time.Millisecond
	consumerRetryMax  = 5 * time.Second
	consumerRetries   = 5
)

// Consume reads records in order and only commits an offset after its handler
// succeeds, so a failed record is never skipped past. Handler failures are
// retried in place with backoff; if they persist, Consume returns an error and
// the caller (a supervisor) restarts the reader, which redelivers from the last
// committed offset. A malformed record can never succeed, so it is committed
// and dropped rather than blocking the partition.
func (k *Kafka) Consume(ctx context.Context, group string, handle func(context.Context, string) error) error {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: k.brokers, Topic: k.topic, GroupID: group, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: time.Second})
	defer r.Close()
	for {
		m, e := r.FetchMessage(ctx)
		if e != nil {
			return e
		}
		var body struct {
			InvocationID string `json:"invocation_id"`
		}
		if e = json.Unmarshal(m.Value, &body); e != nil {
			_ = r.CommitMessages(ctx, m)
			continue
		}
		if e = retryHandler(ctx, handle, body.InvocationID); e != nil {
			// Leave the offset uncommitted: the supervisor will restart the
			// reader and this record is redelivered.
			return e
		}
		if e = r.CommitMessages(ctx, m); e != nil {
			return e
		}
	}
}

func retryHandler(ctx context.Context, handle func(context.Context, string) error, id string) error {
	var err error
	for attempt := 0; ; attempt++ {
		if err = handle(ctx, id); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt >= consumerRetries {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(consumerBackoff(attempt)):
		}
	}
}

func consumerBackoff(attempt int) time.Duration {
	d := consumerRetryBase << attempt
	if d > consumerRetryMax || d <= 0 {
		return consumerRetryMax
	}
	return d
}
