package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/kegazani/metachat-event-sourcing/events"
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

type DiaryEventConsumer interface {
	Start(ctx context.Context) error
	Stop() error
}

type diaryEventConsumer struct {
	consumer     sarama.ConsumerGroup
	topic        string
	processEvent func(ctx context.Context, event *events.Event) error
	logger       *logrus.Logger
	running      bool
	mu           sync.Mutex
}

type consumerGroupHandler struct {
	processEvent func(ctx context.Context, event *events.Event) error
	logger       *logrus.Logger
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			var event events.Event
			if err := json.Unmarshal(message.Value, &event); err != nil {
				h.logger.WithError(err).Error("Failed to unmarshal event")
				session.MarkMessage(message, "")
				continue
			}

			h.logger.WithFields(logrus.Fields{
				"event_type":   event.Type,
				"aggregate_id": event.AggregateID,
				"topic":        message.Topic,
				"partition":    message.Partition,
				"offset":       message.Offset,
			}).Debug("Processing diary event")

			ctx := context.Background()
			if err := h.processEvent(ctx, &event); err != nil {
				h.logger.WithError(err).Error("Failed to process event")
				continue
			}

			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func NewDiaryEventConsumer(
	bootstrapServers string,
	topic string,
	groupID string,
	processEvent func(ctx context.Context, event *events.Event) error,
	logger *logrus.Logger,
) (DiaryEventConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	brokers := strings.Split(bootstrapServers, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &diaryEventConsumer{
		consumer:     consumer,
		topic:        topic,
		processEvent: processEvent,
		logger:       logger,
		running:      false,
	}, nil
}

func (c *diaryEventConsumer) Start(ctx context.Context) error {
	c.mu.Lock()
	c.running = true
	c.mu.Unlock()

	c.logger.WithField("topic", c.topic).Info("Starting Kafka consumer")

	handler := &consumerGroupHandler{
		processEvent: c.processEvent,
		logger:       c.logger,
	}

	go func() {
		for {
			c.mu.Lock()
			running := c.running
			c.mu.Unlock()

			if !running {
				return
			}

			select {
			case <-ctx.Done():
				c.logger.Info("Context cancelled, stopping consumer")
				c.mu.Lock()
				c.running = false
				c.mu.Unlock()
				return
			default:
				topics := []string{c.topic}
				err := c.consumer.Consume(ctx, topics, handler)
				if err != nil {
					c.logger.WithError(err).Error("Error from consumer")
				}
			}
		}
	}()

	go func() {
		for err := range c.consumer.Errors() {
			c.logger.WithError(err).Error("Kafka consumer error")
		}
	}()

	return nil
}

func (c *diaryEventConsumer) Stop() error {
	c.mu.Lock()
	c.running = false
	c.mu.Unlock()

	c.logger.Info("Stopping Kafka consumer")
	return c.consumer.Close()
}
