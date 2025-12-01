package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/metachat/common/event-sourcing/events"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// DiaryEventProducer defines the interface for publishing diary events to Kafka
type DiaryEventProducer interface {
	// PublishDiaryEvent publishes a diary event to Kafka
	PublishDiaryEvent(ctx context.Context, event *events.Event) error

	// Close closes the Kafka producer
	Close() error
}

// diaryEventProducer is the implementation of DiaryEventProducer
type diaryEventProducer struct {
	producer *kafka.Producer
	topic    string
}

// NewDiaryEventProducer creates a new diary event producer
func NewDiaryEventProducer(bootstrapServers, topic string) (DiaryEventProducer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"acks":              "all",
		"retries":           3,
		"retry.backoff.ms":  100,
		"linger.ms":         10,
		"buffer.memory":     33554432, // 32MB
		"compression.type":  "snappy",
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &diaryEventProducer{
		producer: producer,
		topic:    topic,
	}, nil
}

// PublishDiaryEvent publishes a diary event to Kafka
func (p *diaryEventProducer) PublishDiaryEvent(ctx context.Context, event *events.Event) error {
	// Convert event to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Create Kafka message
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            []byte(event.AggregateID),
		Value:          eventJSON,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(event.Type)},
			{Key: "aggregate_id", Value: []byte(event.AggregateID)},
			{Key: "timestamp", Value: []byte(event.Timestamp.Format(time.RFC3339))},
		},
	}

	// Produce message
	deliveryChan := make(chan kafka.Event)
	err = p.producer.Produce(message, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	// Wait for delivery report
	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}

	return nil
}

// Close closes the Kafka producer
func (p *diaryEventProducer) Close() error {
	p.producer.Flush(15 * 1000) // 15 seconds timeout
	p.producer.Close()
	return nil
}
