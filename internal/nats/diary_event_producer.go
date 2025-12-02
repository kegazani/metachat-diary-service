package nats

import (
	"context"

	"metachat/diary-service/internal/bus"
	"github.com/kegazani/metachat-event-sourcing/events"
)

type DiaryEventProducer struct {
	eventBus bus.EventBus
}

func NewDiaryEventProducer(eventBus bus.EventBus) *DiaryEventProducer {
	return &DiaryEventProducer{
		eventBus: eventBus,
	}
}

func (p *DiaryEventProducer) PublishDiaryEvent(ctx context.Context, event *events.Event) error {
	return p.eventBus.Publish(ctx, event)
}

func (p *DiaryEventProducer) Close() error {
	return p.eventBus.Close()
}

