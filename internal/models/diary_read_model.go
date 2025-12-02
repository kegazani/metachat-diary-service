package models

import (
	"time"

	"github.com/kegazani/metachat-event-sourcing/events"
)

// DiaryEntryReadModel represents the read model for diary entries in Cassandra
type DiaryEntryReadModel struct {
	ID         string    `cql:"id"`
	UserID     string    `cql:"user_id"`
	Title      string    `cql:"title"`
	Content    string    `cql:"content"`
	TokenCount int       `cql:"token_count"`
	SessionID  string    `cql:"session_id"`
	Tags       []string  `cql:"tags"`
	Deleted    bool      `cql:"deleted"`
	CreatedAt  time.Time `cql:"created_at"`
	UpdatedAt  time.Time `cql:"updated_at"`
	Version    int       `cql:"version"`
}

// DiaryEntryByUserReadModel represents a read model optimized for user-based lookups
type DiaryEntryByUserReadModel struct {
	UserID     string    `cql:"user_id"`
	EntryID    string    `cql:"entry_id"`
	Title      string    `cql:"title"`
	CreatedAt  time.Time `cql:"created_at"`
	TokenCount int       `cql:"token_count"`
	Tags       []string  `cql:"tags"`
	Deleted    bool      `cql:"deleted"`
}

// DiaryEntryByUserAndTimeReadModel represents a read model optimized for user and time-based lookups
type DiaryEntryByUserAndTimeReadModel struct {
	UserID     string    `cql:"user_id"`
	YearMonth  string    `cql:"year_month"` // Format: YYYY-MM
	EntryID    string    `cql:"entry_id"`
	Title      string    `cql:"title"`
	CreatedAt  time.Time `cql:"created_at"`
	TokenCount int       `cql:"token_count"`
	Tags       []string  `cql:"tags"`
	Deleted    bool      `cql:"deleted"`
}

// DiarySessionReadModel represents the read model for diary sessions
type DiarySessionReadModel struct {
	ID         string    `cql:"id"`
	UserID     string    `cql:"user_id"`
	StartTime  time.Time `cql:"start_time"`
	EndTime    time.Time `cql:"end_time"`
	Source     string    `cql:"source"`
	EntryCount int       `cql:"entry_count"`
	TokenCount int       `cql:"token_count"`
	CreatedAt  time.Time `cql:"created_at"`
	UpdatedAt  time.Time `cql:"updated_at"`
}

// NewDiaryEntryReadModelFromEvent creates a new DiaryEntryReadModel from a DiaryEntryCreated event
func NewDiaryEntryReadModelFromEvent(event *events.Event) (*DiaryEntryReadModel, error) {
	var payload events.DiaryEntryCreatedPayload
	if err := event.UnmarshalPayload(&payload); err != nil {
		return nil, err
	}

	return &DiaryEntryReadModel{
		ID:         event.AggregateID,
		UserID:     payload.UserID,
		Title:      payload.Title,
		Content:    payload.Content,
		TokenCount: payload.TokenCount,
		SessionID:  payload.SessionID,
		Tags:       payload.Tags,
		Deleted:    false,
		CreatedAt:  event.Timestamp,
		UpdatedAt:  event.Timestamp,
		Version:    event.Version,
	}, nil
}

// NewDiaryEntryByUserReadModelFromEvent creates a new DiaryEntryByUserReadModel from a DiaryEntryCreated event
func NewDiaryEntryByUserReadModelFromEvent(event *events.Event) (*DiaryEntryByUserReadModel, error) {
	var payload events.DiaryEntryCreatedPayload
	if err := event.UnmarshalPayload(&payload); err != nil {
		return nil, err
	}

	return &DiaryEntryByUserReadModel{
		UserID:     payload.UserID,
		EntryID:    event.AggregateID,
		Title:      payload.Title,
		CreatedAt:  event.Timestamp,
		TokenCount: payload.TokenCount,
		Tags:       payload.Tags,
		Deleted:    false,
	}, nil
}

// NewDiaryEntryByUserAndTimeReadModelFromEvent creates a new DiaryEntryByUserAndTimeReadModel from a DiaryEntryCreated event
func NewDiaryEntryByUserAndTimeReadModelFromEvent(event *events.Event) (*DiaryEntryByUserAndTimeReadModel, error) {
	var payload events.DiaryEntryCreatedPayload
	if err := event.UnmarshalPayload(&payload); err != nil {
		return nil, err
	}

	yearMonth := event.Timestamp.Format("2006-01")

	return &DiaryEntryByUserAndTimeReadModel{
		UserID:     payload.UserID,
		YearMonth:  yearMonth,
		EntryID:    event.AggregateID,
		Title:      payload.Title,
		CreatedAt:  event.Timestamp,
		TokenCount: payload.TokenCount,
		Tags:       payload.Tags,
		Deleted:    false,
	}, nil
}

// NewDiarySessionReadModelFromEvents creates a new DiarySessionReadModel from DiarySessionStarted and DiarySessionEnded events
func NewDiarySessionReadModelFromEvents(startEvent, endEvent *events.Event) (*DiarySessionReadModel, error) {
	var startPayload events.DiarySessionStartedPayload
	if err := startEvent.UnmarshalPayload(&startPayload); err != nil {
		return nil, err
	}

	session := &DiarySessionReadModel{
		ID:        startEvent.AggregateID,
		UserID:    startPayload.UserID,
		StartTime: startEvent.Timestamp,
		Source:    startPayload.Source,
		CreatedAt: startEvent.Timestamp,
		UpdatedAt: startEvent.Timestamp,
	}

	if endEvent != nil {
		var endPayload events.DiarySessionEndedPayload
		if err := endEvent.UnmarshalPayload(&endPayload); err != nil {
			return nil, err
		}

		session.EndTime = endEvent.Timestamp
		session.EntryCount = endPayload.EntryCount
		session.TokenCount = endPayload.TokenCount
		session.UpdatedAt = endEvent.Timestamp
	}

	return session, nil
}

// UpdateFromUpdateEvent updates the DiaryEntryReadModel from a DiaryEntryUpdated event
func (d *DiaryEntryReadModel) UpdateFromUpdateEvent(event *events.Event) error {
	var payload events.DiaryEntryUpdatedPayload
	if err := event.UnmarshalPayload(&payload); err != nil {
		return err
	}

	if payload.Title != "" {
		d.Title = payload.Title
	}
	if payload.Content != "" {
		d.Content = payload.Content
	}
	if payload.TokenCount > 0 {
		d.TokenCount = payload.TokenCount
	}
	if payload.Tags != nil {
		d.Tags = payload.Tags
	}

	d.UpdatedAt = event.Timestamp
	d.Version = event.Version
	return nil
}

// UpdateFromDeleteEvent updates the DiaryEntryReadModel from a DiaryEntryDeleted event
func (d *DiaryEntryReadModel) UpdateFromDeleteEvent(event *events.Event) error {
	var payload events.DiaryEntryDeletedPayload
	if err := event.UnmarshalPayload(&payload); err != nil {
		return err
	}

	d.Deleted = true
	d.UpdatedAt = event.Timestamp
	d.Version = event.Version
	return nil
}

// UpdateFromUpdateEvent updates the DiaryEntryByUserReadModel from a DiaryEntryUpdated event
func (d *DiaryEntryByUserReadModel) UpdateFromUpdateEvent(event *events.Event) error {
	var payload events.DiaryEntryUpdatedPayload
	if err := event.UnmarshalPayload(&payload); err != nil {
		return err
	}

	if payload.Title != "" {
		d.Title = payload.Title
	}
	if payload.TokenCount > 0 {
		d.TokenCount = payload.TokenCount
	}
	if payload.Tags != nil {
		d.Tags = payload.Tags
	}

	return nil
}

// UpdateFromDeleteEvent updates the DiaryEntryByUserReadModel from a DiaryEntryDeleted event
func (d *DiaryEntryByUserReadModel) UpdateFromDeleteEvent(event *events.Event) error {
	d.Deleted = true
	return nil
}

// UpdateFromUpdateEvent updates the DiaryEntryByUserAndTimeReadModel from a DiaryEntryUpdated event
func (d *DiaryEntryByUserAndTimeReadModel) UpdateFromUpdateEvent(event *events.Event) error {
	var payload events.DiaryEntryUpdatedPayload
	if err := event.UnmarshalPayload(&payload); err != nil {
		return err
	}

	if payload.Title != "" {
		d.Title = payload.Title
	}
	if payload.TokenCount > 0 {
		d.TokenCount = payload.TokenCount
	}
	if payload.Tags != nil {
		d.Tags = payload.Tags
	}

	return nil
}

// UpdateFromDeleteEvent updates the DiaryEntryByUserAndTimeReadModel from a DiaryEntryDeleted event
func (d *DiaryEntryByUserAndTimeReadModel) UpdateFromDeleteEvent(event *events.Event) error {
	d.Deleted = true
	return nil
}

// UpdateFromEndEvent updates the DiarySessionReadModel from a DiarySessionEnded event
func (d *DiarySessionReadModel) UpdateFromEndEvent(event *events.Event) error {
	var payload events.DiarySessionEndedPayload
	if err := event.UnmarshalPayload(&payload); err != nil {
		return err
	}

	d.EndTime = event.Timestamp
	d.EntryCount = payload.EntryCount
	d.TokenCount = payload.TokenCount
	d.UpdatedAt = event.Timestamp
	return nil
}
