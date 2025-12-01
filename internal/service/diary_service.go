package service

import (
	"context"
	"fmt"
	"time"

	"github.com/metachat/common/event-sourcing/aggregates"
	"metachat/diary-service/internal/kafka"
	"metachat/diary-service/internal/models"
	"metachat/diary-service/internal/repository"

	"github.com/sirupsen/logrus"
)

// DiaryService defines the interface for diary service operations
type DiaryService interface {
	// CreateDiaryEntry creates a new diary entry
	CreateDiaryEntry(ctx context.Context, userID, title, content string, tokenCount int, sessionID string, tags []string) (*aggregates.DiaryAggregate, error)

	// UpdateDiaryEntry updates an existing diary entry
	UpdateDiaryEntry(ctx context.Context, diaryID, title, content string, tokenCount int, tags []string) (*aggregates.DiaryAggregate, error)

	// DeleteDiaryEntry deletes a diary entry
	DeleteDiaryEntry(ctx context.Context, diaryID string) error

	// GetDiaryEntryByID retrieves a diary entry by ID
	GetDiaryEntryByID(ctx context.Context, diaryID string) (*aggregates.DiaryAggregate, error)

	// GetDiaryEntriesByUserID retrieves all diary entries for a user
	GetDiaryEntriesByUserID(ctx context.Context, userID string) ([]*aggregates.DiaryAggregate, error)

	// GetDiaryEntriesByUserIDAndTimeRange retrieves diary entries for a user within a time range
	GetDiaryEntriesByUserIDAndTimeRange(ctx context.Context, userID, startTime, endTime string) ([]*aggregates.DiaryAggregate, error)

	// GetDiaryEntryReadModelByID retrieves a diary entry read model by ID
	GetDiaryEntryReadModelByID(ctx context.Context, diaryID string) (*models.DiaryEntryReadModel, error)

	// GetDiaryEntryReadModelsByUserID retrieves all diary entry read models for a user
	GetDiaryEntryReadModelsByUserID(ctx context.Context, userID string) ([]*models.DiaryEntryReadModel, error)

	// GetDiaryEntryReadModelsByUserIDAndTimeRange retrieves diary entry read models for a user within a time range
	GetDiaryEntryReadModelsByUserIDAndTimeRange(ctx context.Context, userID string, startTime, endTime time.Time) ([]*models.DiaryEntryReadModel, error)

	// GetDiarySessionReadModelsByUserID retrieves all diary session read models for a user
	GetDiarySessionReadModelsByUserID(ctx context.Context, userID string) ([]*models.DiarySessionReadModel, error)
}

// diaryService is the implementation of DiaryService
type diaryService struct {
	diaryRepository     repository.DiaryRepository
	diaryReadRepository repository.DiaryReadRepository
	diaryEventProducer  kafka.DiaryEventProducer
}

// NewDiaryService creates a new diary service
func NewDiaryService(diaryRepository repository.DiaryRepository, diaryReadRepository repository.DiaryReadRepository, diaryEventProducer kafka.DiaryEventProducer) DiaryService {
	return &diaryService{
		diaryRepository:     diaryRepository,
		diaryReadRepository: diaryReadRepository,
		diaryEventProducer:  diaryEventProducer,
	}
}

// CreateDiaryEntry creates a new diary entry
func (s *diaryService) CreateDiaryEntry(ctx context.Context, userID, title, content string, tokenCount int, sessionID string, tags []string) (*aggregates.DiaryAggregate, error) {
	// Create new diary aggregate
	diary := aggregates.NewDiaryAggregate("")

	// Create diary entry
	if err := diary.CreateEntry(userID, title, content, tokenCount, sessionID, tags); err != nil {
		return nil, err
	}

	// Save diary
	if err := s.diaryRepository.SaveDiary(ctx, diary); err != nil {
		return nil, err
	}

	// Publish event to Kafka
	if err := s.publishDiaryEvents(ctx, diary); err != nil {
		logrus.WithError(err).Error("Failed to publish diary events to Kafka")
	}

	return diary, nil
}

// UpdateDiaryEntry updates an existing diary entry
func (s *diaryService) UpdateDiaryEntry(ctx context.Context, diaryID, title, content string, tokenCount int, tags []string) (*aggregates.DiaryAggregate, error) {
	// Get diary by ID
	diary, err := s.diaryRepository.GetDiaryByID(ctx, diaryID)
	if err != nil {
		return nil, err
	}

	// Update diary entry
	if err := diary.UpdateEntry(title, content, tokenCount, tags); err != nil {
		return nil, err
	}

	// Save diary
	if err := s.diaryRepository.SaveDiary(ctx, diary); err != nil {
		return nil, err
	}

	// Publish event to Kafka
	if err := s.publishDiaryEvents(ctx, diary); err != nil {
		logrus.WithError(err).Error("Failed to publish diary events to Kafka")
	}

	return diary, nil
}

// DeleteDiaryEntry deletes a diary entry
func (s *diaryService) DeleteDiaryEntry(ctx context.Context, diaryID string) error {
	// Get diary by ID
	diary, err := s.diaryRepository.GetDiaryByID(ctx, diaryID)
	if err != nil {
		return err
	}

	// Delete diary entry
	if err := diary.DeleteEntry("user requested deletion"); err != nil {
		return err
	}

	// Save diary
	if err := s.diaryRepository.SaveDiary(ctx, diary); err != nil {
		return err
	}

	// Publish event to Kafka
	if err := s.publishDiaryEvents(ctx, diary); err != nil {
		logrus.WithError(err).Error("Failed to publish diary events to Kafka")
	}

	return nil
}

// GetDiaryEntryByID retrieves a diary entry by ID
func (s *diaryService) GetDiaryEntryByID(ctx context.Context, diaryID string) (*aggregates.DiaryAggregate, error) {
	return s.diaryRepository.GetDiaryByID(ctx, diaryID)
}

// GetDiaryEntriesByUserID retrieves all diary entries for a user
func (s *diaryService) GetDiaryEntriesByUserID(ctx context.Context, userID string) ([]*aggregates.DiaryAggregate, error) {
	return s.diaryRepository.GetDiariesByUserID(ctx, userID)
}

// GetDiaryEntriesByUserIDAndTimeRange retrieves diary entries for a user within a time range
func (s *diaryService) GetDiaryEntriesByUserIDAndTimeRange(ctx context.Context, userID, startTime, endTime string) ([]*aggregates.DiaryAggregate, error) {
	return s.diaryRepository.GetDiariesByUserIDAndTimeRange(ctx, userID, startTime, endTime)
}

// GetDiaryEntryReadModelByID retrieves a diary entry read model by ID
func (s *diaryService) GetDiaryEntryReadModelByID(ctx context.Context, diaryID string) (*models.DiaryEntryReadModel, error) {
	return s.diaryReadRepository.GetDiaryEntryByID(ctx, diaryID)
}

// GetDiaryEntryReadModelsByUserID retrieves all diary entry read models for a user
func (s *diaryService) GetDiaryEntryReadModelsByUserID(ctx context.Context, userID string) ([]*models.DiaryEntryReadModel, error) {
	return s.diaryReadRepository.GetDiaryEntriesByUserID(ctx, userID)
}

// GetDiaryEntryReadModelsByUserIDAndTimeRange retrieves diary entry read models for a user within a time range
func (s *diaryService) GetDiaryEntryReadModelsByUserIDAndTimeRange(ctx context.Context, userID string, startTime, endTime time.Time) ([]*models.DiaryEntryReadModel, error) {
	return s.diaryReadRepository.GetDiaryEntriesByUserIDAndTimeRange(ctx, userID, startTime, endTime)
}

// GetDiarySessionReadModelsByUserID retrieves all diary session read models for a user
func (s *diaryService) GetDiarySessionReadModelsByUserID(ctx context.Context, userID string) ([]*models.DiarySessionReadModel, error) {
	return s.diaryReadRepository.GetDiarySessionsByUserID(ctx, userID)
}

// publishDiaryEvents publishes all uncommitted events for a diary to Kafka
func (s *diaryService) publishDiaryEvents(ctx context.Context, diary *aggregates.DiaryAggregate) error {
	events := diary.GetUncommittedEvents()
	for _, event := range events {
		if err := s.diaryEventProducer.PublishDiaryEvent(ctx, event); err != nil {
			return fmt.Errorf("failed to publish diary event: %w", err)
		}
	}

	// Mark events as committed
	diary.ClearUncommittedEvents()
	return nil
}
