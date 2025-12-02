package repository

import (
	"context"

	"github.com/kegazani/metachat-event-sourcing/aggregates"
	"github.com/kegazani/metachat-event-sourcing/events"
	"github.com/kegazani/metachat-event-sourcing/serializer"
	"github.com/kegazani/metachat-event-sourcing/store"
)

// DiaryRepository defines the interface for diary repository operations
type DiaryRepository interface {
	// SaveDiary saves a diary aggregate to the event store
	SaveDiary(ctx context.Context, diary *aggregates.DiaryAggregate) error

	// GetDiaryByID retrieves a diary aggregate by ID
	GetDiaryByID(ctx context.Context, diaryID string) (*aggregates.DiaryAggregate, error)

	// GetDiariesByUserID retrieves all diary entries for a specific user
	GetDiariesByUserID(ctx context.Context, userID string) ([]*aggregates.DiaryAggregate, error)

	// GetDiariesByUserIDAndTimeRange retrieves diary entries for a user within a time range
	GetDiariesByUserIDAndTimeRange(ctx context.Context, userID, startTime, endTime string) ([]*aggregates.DiaryAggregate, error)
}

// diaryRepository is the implementation of DiaryRepository
type diaryRepository struct {
	eventStore store.EventStore
	serializer serializer.Serializer
}

// NewDiaryRepository creates a new diary repository
func NewDiaryRepository(eventStore store.EventStore, serializer serializer.Serializer) DiaryRepository {
	return &diaryRepository{
		eventStore: eventStore,
		serializer: serializer,
	}
}

// SaveDiary saves a diary aggregate to the event store
func (r *diaryRepository) SaveDiary(ctx context.Context, diary *aggregates.DiaryAggregate) error {
	// Get uncommitted events
	eventList := diary.GetUncommittedEvents()
	if len(eventList) == 0 {
		return nil
	}

	// Save events to event store
	if err := r.eventStore.SaveEvents(ctx, eventList); err != nil {
		return err
	}

	// Clear uncommitted events
	diary.ClearUncommittedEvents()
	return nil
}

// GetDiaryByID retrieves a diary aggregate by ID
func (r *diaryRepository) GetDiaryByID(ctx context.Context, diaryID string) (*aggregates.DiaryAggregate, error) {
	// Get events from event store
	events, err := r.eventStore.GetEventsByAggregateID(ctx, diaryID)
	if err != nil {
		return nil, err
	}

	// Create diary aggregate
	diary := aggregates.NewDiaryAggregate(diaryID)

	// Load events into aggregate
	if err := diary.LoadFromHistory(events); err != nil {
		return nil, err
	}

	return diary, nil
}

// GetDiariesByUserID retrieves all diary entries for a specific user
func (r *diaryRepository) GetDiariesByUserID(ctx context.Context, userID string) ([]*aggregates.DiaryAggregate, error) {
	// Get all DiaryEntryCreated events
	eventList, err := r.eventStore.GetEventsByType(ctx, events.DiaryEntryCreatedEvent)
	if err != nil {
		return nil, err
	}

	var diaries []*aggregates.DiaryAggregate

	// Find the diary entries with the matching user ID
	for _, event := range eventList {
		var payload events.DiaryEntryCreatedPayload
		if err := event.UnmarshalPayload(&payload); err != nil {
			continue
		}

		if payload.UserID == userID {
			// Create diary aggregate
			diary := aggregates.NewDiaryAggregate(event.AggregateID)

			// Load events into aggregate
			if err := diary.LoadFromHistory([]*events.Event{event}); err != nil {
				return nil, err
			}

			diaries = append(diaries, diary)
		}
	}

	return diaries, nil
}

// GetDiariesByUserIDAndTimeRange retrieves diary entries for a user within a time range
func (r *diaryRepository) GetDiariesByUserIDAndTimeRange(ctx context.Context, userID, startTime, endTime string) ([]*aggregates.DiaryAggregate, error) {
	// Get events within time range
	eventList, err := r.eventStore.GetEventsByTimeRange(ctx, startTime, endTime)
	if err != nil {
		return nil, err
	}

	var diaries []*aggregates.DiaryAggregate

	// Find the diary entries with the matching user ID
	for _, event := range eventList {
		var payload events.DiaryEntryCreatedPayload
		if err := event.UnmarshalPayload(&payload); err != nil {
			continue
		}

		if payload.UserID == userID {
			// Create diary aggregate
			diary := aggregates.NewDiaryAggregate(event.AggregateID)

			// Load events into aggregate
			if err := diary.LoadFromHistory([]*events.Event{event}); err != nil {
				return nil, err
			}

			diaries = append(diaries, diary)
		}
	}

	return diaries, nil
}
