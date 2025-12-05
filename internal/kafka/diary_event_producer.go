package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kegazani/metachat-event-sourcing/events"
	"github.com/IBM/sarama"
)

type DiaryEventProducer interface {
	PublishDiaryEvent(ctx context.Context, event *events.Event) error
	Close() error
}

type diaryEventProducer struct {
	producer     sarama.SyncProducer
	defaultTopic string
	topicMap     map[events.EventType]string
}

func NewDiaryEventProducer(bootstrapServers string, defaultTopic string, topicMap map[events.EventType]string) (DiaryEventProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Timeout = 10 * time.Second

	brokers := strings.Split(bootstrapServers, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &diaryEventProducer{
		producer:     producer,
		defaultTopic: defaultTopic,
		topicMap:     topicMap,
	}, nil
}

func (p *diaryEventProducer) getTopicForEvent(eventType events.EventType) string {
	if topic, ok := p.topicMap[eventType]; ok {
		return topic
	}
	return p.defaultTopic
}

func (p *diaryEventProducer) PublishDiaryEvent(ctx context.Context, event *events.Event) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	topic := p.getTopicForEvent(event.Type)

	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(event.AggregateID),
		Value: sarama.ByteEncoder(eventJSON),
		Headers: []sarama.RecordHeader{
			{Key: []byte("event_type"), Value: []byte(event.Type)},
			{Key: []byte("aggregate_id"), Value: []byte(event.AggregateID)},
			{Key: []byte("timestamp"), Value: []byte(event.Timestamp.Format(time.RFC3339))},
		},
	}

	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	_ = partition
	_ = offset

	return nil
}

func (p *diaryEventProducer) Close() error {
	return p.producer.Close()
}
