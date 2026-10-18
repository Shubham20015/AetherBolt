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
		if e = handle(ctx, body.InvocationID); e != nil {
			continue
		}
		if e = r.CommitMessages(ctx, m); e != nil {
			return e
		}
	}
}
