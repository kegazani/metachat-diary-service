package bus

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kegazani/metachat-event-sourcing/events"
	"github.com/nats-io/nats.go"
)

type EventBus interface {
	Publish(ctx context.Context, event *events.Event) error
	Subscribe(eventType events.EventType, handler EventHandler) error
	Close() error
}

type EventHandler func(ctx context.Context, event *events.Event) error

type NATSEventBus struct {
	conn    *nats.Conn
	js      nats.JetStreamContext
	subs    map[events.EventType]*nats.Subscription
	subject string
}

func NewNATSEventBus(url, subject string) (*NATSEventBus, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	streamName := "EVENTS"
	_, err = js.StreamInfo(streamName)
	if err == nats.ErrStreamNotFound {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{subject + ".>"},
			Replicas: 1,
		})
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to create stream: %w", err)
		}
	}

	return &NATSEventBus{
		conn:    conn,
		js:      js,
		subs:    make(map[events.EventType]*nats.Subscription),
		subject: subject,
	}, nil
}

func (b *NATSEventBus) Publish(ctx context.Context, event *events.Event) error {
	subject := fmt.Sprintf("%s.%s", b.subject, string(event.Type))

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	_, err = b.js.Publish(subject, eventJSON)
	if err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	return nil
}

func (b *NATSEventBus) Subscribe(eventType events.EventType, handler EventHandler) error {
	subject := fmt.Sprintf("%s.%s", b.subject, string(eventType))

	sub, err := b.js.Subscribe(subject, func(msg *nats.Msg) {
		var event events.Event
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			return
		}

		ctx := context.Background()
		if err := handler(ctx, &event); err != nil {
			return
		}

		msg.Ack()
	}, nats.Durable(fmt.Sprintf("handler-%s", string(eventType))))

	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	b.subs[eventType] = sub
	return nil
}

func (b *NATSEventBus) Close() error {
	for _, sub := range b.subs {
		if err := sub.Unsubscribe(); err != nil {
			return err
		}
	}

	b.conn.Close()
	return nil
}


