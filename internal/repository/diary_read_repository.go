package repository

import (
	"context"
	"fmt"
	"time"

	"metachat/diary-service/internal/models"

	"github.com/kegazani/metachat-event-sourcing/events"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

// DiaryReadRepository defines the interface for diary read model operations
type DiaryReadRepository interface {
	// SaveDiaryEntry saves a diary entry read model to Cassandra
	SaveDiaryEntry(ctx context.Context, entry *models.DiaryEntryReadModel) error

	// SaveDiaryEntryByUser saves a diary entry by user read model to Cassandra
	SaveDiaryEntryByUser(ctx context.Context, entryByUser *models.DiaryEntryByUserReadModel) error

	// SaveDiaryEntryByUserAndTime saves a diary entry by user and time read model to Cassandra
	SaveDiaryEntryByUserAndTime(ctx context.Context, entryByUserAndTime *models.DiaryEntryByUserAndTimeReadModel) error

	// SaveDiarySession saves a diary session read model to Cassandra
	SaveDiarySession(ctx context.Context, session *models.DiarySessionReadModel) error

	// GetDiaryEntryByID retrieves a diary entry read model by ID
	GetDiaryEntryByID(ctx context.Context, entryID string) (*models.DiaryEntryReadModel, error)

	// GetDiaryEntriesByUserID retrieves all diary entries for a user
	GetDiaryEntriesByUserID(ctx context.Context, userID string) ([]*models.DiaryEntryReadModel, error)

	// GetDiaryEntriesByUserIDAndTimeRange retrieves diary entries for a user within a time range
	GetDiaryEntriesByUserIDAndTimeRange(ctx context.Context, userID string, startTime, endTime time.Time) ([]*models.DiaryEntryReadModel, error)

	// GetDiarySessionsByUserID retrieves all diary sessions for a user
	GetDiarySessionsByUserID(ctx context.Context, userID string) ([]*models.DiarySessionReadModel, error)

	// UpdateDiaryEntry updates a diary entry read model in Cassandra
	UpdateDiaryEntry(ctx context.Context, entry *models.DiaryEntryReadModel) error

	// UpdateDiarySession updates a diary session read model in Cassandra
	UpdateDiarySession(ctx context.Context, session *models.DiarySessionReadModel) error

	// DeleteDiaryEntry deletes a diary entry read model from Cassandra
	DeleteDiaryEntry(ctx context.Context, entryID string) error

	// ProcessDiaryEvent processes a diary event and updates the read models accordingly
	ProcessDiaryEvent(ctx context.Context, event *events.Event) error

	InitializeTables() error
}

// diaryReadRepository is the implementation of DiaryReadRepository
type diaryReadRepository struct {
	session *gocql.Session
}

// NewDiaryReadRepository creates a new diary read repository
func NewDiaryReadRepository(session *gocql.Session) DiaryReadRepository {
	return &diaryReadRepository{
		session: session,
	}
}

// stringToUUID converts a string to UUID. If string is already a valid UUID, returns it.
// Otherwise, generates a deterministic UUID v5 based on the string.
func stringToUUID(s string) (gocql.UUID, error) {
	if s == "" {
		return gocql.UUID{}, fmt.Errorf("empty string cannot be converted to UUID")
	}

	parsedUUID, err := uuid.Parse(s)
	if err == nil {
		return gocql.UUID(parsedUUID), nil
	}

	namespace := uuid.NameSpaceDNS
	deterministicUUID := uuid.NewSHA1(namespace, []byte(s))
	return gocql.UUID(deterministicUUID), nil
}

// SaveDiaryEntry saves a diary entry read model to Cassandra
func (r *diaryReadRepository) SaveDiaryEntry(ctx context.Context, entry *models.DiaryEntryReadModel) error {
	query := `INSERT INTO diary_entries_read_model (id, user_id, title, content, token_count, 
		session_id, tags, deleted, created_at, updated_at, version) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	idUUID, err := stringToUUID(entry.ID)
	if err != nil {
		return fmt.Errorf("failed to convert entry ID to UUID: %w", err)
	}

	userIDUUID, err := stringToUUID(entry.UserID)
	if err != nil {
		return fmt.Errorf("failed to convert user ID to UUID: %w", err)
	}

	var sessionIDUUID gocql.UUID
	if entry.SessionID != "" {
		sessionIDUUID, err = stringToUUID(entry.SessionID)
		if err != nil {
			return fmt.Errorf("failed to convert session ID to UUID: %w", err)
		}
	}

	return r.session.Query(query,
		idUUID, userIDUUID, entry.Title, entry.Content, entry.TokenCount,
		sessionIDUUID, entry.Tags, entry.Deleted, entry.CreatedAt, entry.UpdatedAt, entry.Version,
	).Exec()
}

// SaveDiaryEntryByUser saves a diary entry by user read model to Cassandra
func (r *diaryReadRepository) SaveDiaryEntryByUser(ctx context.Context, entryByUser *models.DiaryEntryByUserReadModel) error {
	query := `INSERT INTO diary_entries_by_user_read_model (user_id, entry_id, title, 
		created_at, token_count, tags, deleted) 
		VALUES (?, ?, ?, ?, ?, ?, ?)`

	userIDUUID, err := stringToUUID(entryByUser.UserID)
	if err != nil {
		return fmt.Errorf("failed to convert user ID to UUID: %w", err)
	}

	entryIDUUID, err := stringToUUID(entryByUser.EntryID)
	if err != nil {
		return fmt.Errorf("failed to convert entry ID to UUID: %w", err)
	}

	return r.session.Query(query,
		userIDUUID, entryIDUUID, entryByUser.Title,
		entryByUser.CreatedAt, entryByUser.TokenCount, entryByUser.Tags, entryByUser.Deleted,
	).Exec()
}

// SaveDiaryEntryByUserAndTime saves a diary entry by user and time read model to Cassandra
func (r *diaryReadRepository) SaveDiaryEntryByUserAndTime(ctx context.Context, entryByUserAndTime *models.DiaryEntryByUserAndTimeReadModel) error {
	query := `INSERT INTO diary_entries_by_user_and_time_read_model (user_id, year_month, entry_id, 
		title, created_at, token_count, tags, deleted) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	userIDUUID, err := stringToUUID(entryByUserAndTime.UserID)
	if err != nil {
		return fmt.Errorf("failed to convert user ID to UUID: %w", err)
	}

	entryIDUUID, err := stringToUUID(entryByUserAndTime.EntryID)
	if err != nil {
		return fmt.Errorf("failed to convert entry ID to UUID: %w", err)
	}

	return r.session.Query(query,
		userIDUUID, entryByUserAndTime.YearMonth, entryIDUUID,
		entryByUserAndTime.Title, entryByUserAndTime.CreatedAt, entryByUserAndTime.TokenCount,
		entryByUserAndTime.Tags, entryByUserAndTime.Deleted,
	).Exec()
}

// SaveDiarySession saves a diary session read model to Cassandra
func (r *diaryReadRepository) SaveDiarySession(ctx context.Context, session *models.DiarySessionReadModel) error {
	query := `INSERT INTO diary_sessions_read_model (id, user_id, start_time, end_time, 
		source, entry_count, token_count, created_at, updated_at) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	idUUID, err := stringToUUID(session.ID)
	if err != nil {
		return fmt.Errorf("failed to convert session ID to UUID: %w", err)
	}

	userIDUUID, err := stringToUUID(session.UserID)
	if err != nil {
		return fmt.Errorf("failed to convert user ID to UUID: %w", err)
	}

	return r.session.Query(query,
		idUUID, userIDUUID, session.StartTime, session.EndTime,
		session.Source, session.EntryCount, session.TokenCount, session.CreatedAt, session.UpdatedAt,
	).Exec()
}

// GetDiaryEntryByID retrieves a diary entry read model by ID
func (r *diaryReadRepository) GetDiaryEntryByID(ctx context.Context, entryID string) (*models.DiaryEntryReadModel, error) {
	query := `SELECT id, user_id, title, content, token_count, session_id, tags, 
		deleted, created_at, updated_at, version 
		FROM diary_entries_read_model WHERE id = ?`

	entryIDUUID, err := stringToUUID(entryID)
	if err != nil {
		return nil, fmt.Errorf("failed to convert entry ID to UUID: %w", err)
	}

	var idUUID, userIDUUID, sessionIDUUID gocql.UUID
	var entry models.DiaryEntryReadModel
	err = r.session.Query(query, entryIDUUID).Consistency(gocql.One).Scan(
		&idUUID, &userIDUUID, &entry.Title, &entry.Content, &entry.TokenCount,
		&sessionIDUUID, &entry.Tags, &entry.Deleted, &entry.CreatedAt, &entry.UpdatedAt, &entry.Version,
	)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, fmt.Errorf("diary entry not found: %w", err)
		}
		return nil, fmt.Errorf("failed to get diary entry: %w", err)
	}

	entry.ID = uuid.UUID(idUUID).String()
	entry.UserID = uuid.UUID(userIDUUID).String()
	var zeroUUID gocql.UUID
	if sessionIDUUID != zeroUUID {
		entry.SessionID = uuid.UUID(sessionIDUUID).String()
	} else {
		entry.SessionID = ""
	}

	return &entry, nil
}

// GetDiaryEntriesByUserID retrieves all diary entries for a user
func (r *diaryReadRepository) GetDiaryEntriesByUserID(ctx context.Context, userID string) ([]*models.DiaryEntryReadModel, error) {
	query := `SELECT entry_id FROM diary_entries_by_user_read_model WHERE user_id = ?`

	userIDUUID, err := stringToUUID(userID)
	if err != nil {
		return nil, fmt.Errorf("failed to convert user ID to UUID: %w", err)
	}

	iter := r.session.Query(query, userIDUUID).Consistency(gocql.One).Iter()
	var entryIDs []gocql.UUID
	var entryIDUUID gocql.UUID
	for iter.Scan(&entryIDUUID) {
		entryIDs = append(entryIDs, entryIDUUID)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to get entry IDs: %w", err)
	}

	var entries []*models.DiaryEntryReadModel
	for _, idUUID := range entryIDs {
		entryID := uuid.UUID(idUUID).String()
		entry, err := r.GetDiaryEntryByID(ctx, entryID)
		if err != nil {
			return nil, fmt.Errorf("failed to get diary entry %s: %w", entryID, err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// GetDiaryEntriesByUserIDAndTimeRange retrieves diary entries for a user within a time range
func (r *diaryReadRepository) GetDiaryEntriesByUserIDAndTimeRange(ctx context.Context, userID string, startTime, endTime time.Time) ([]*models.DiaryEntryReadModel, error) {
	userIDUUID, err := stringToUUID(userID)
	if err != nil {
		return nil, fmt.Errorf("failed to convert user ID to UUID: %w", err)
	}

	// Generate year-month combinations for the time range
	var yearMonths []string
	current := startTime
	for current.Before(endTime) || current.Equal(endTime) {
		yearMonths = append(yearMonths, current.Format("2006-01"))
		current = current.AddDate(0, 1, 0)
	}

	var entryIDs []gocql.UUID
	for _, yearMonth := range yearMonths {
		query := `SELECT entry_id FROM diary_entries_by_user_and_time_read_model 
			WHERE user_id = ? AND year_month = ?`

		iter := r.session.Query(query, userIDUUID, yearMonth).Consistency(gocql.One).Iter()
		var idUUID gocql.UUID
		for iter.Scan(&idUUID) {
			entryIDs = append(entryIDs, idUUID)
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("failed to get entry IDs for %s: %w", yearMonth, err)
		}
	}

	var entries []*models.DiaryEntryReadModel
	for _, idUUID := range entryIDs {
		entryID := uuid.UUID(idUUID).String()
		entry, err := r.GetDiaryEntryByID(ctx, entryID)
		if err != nil {
			return nil, fmt.Errorf("failed to get diary entry %s: %w", entryID, err)
		}
		// Filter by exact time range
		if (entry.CreatedAt.Equal(startTime) || entry.CreatedAt.After(startTime)) &&
			(entry.CreatedAt.Equal(endTime) || entry.CreatedAt.Before(endTime)) {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// GetDiarySessionsByUserID retrieves all diary sessions for a user
func (r *diaryReadRepository) GetDiarySessionsByUserID(ctx context.Context, userID string) ([]*models.DiarySessionReadModel, error) {
	query := `SELECT id, user_id, start_time, end_time, source, entry_count, 
		token_count, created_at, updated_at 
		FROM diary_sessions_read_model WHERE user_id = ?`

	userIDUUID, err := stringToUUID(userID)
	if err != nil {
		return nil, fmt.Errorf("failed to convert user ID to UUID: %w", err)
	}

	iter := r.session.Query(query, userIDUUID).Consistency(gocql.One).Iter()
	var sessions []*models.DiarySessionReadModel
	var idUUID, userIDUUIDResult gocql.UUID
	var session models.DiarySessionReadModel
	for iter.Scan(
		&idUUID, &userIDUUIDResult, &session.StartTime, &session.EndTime,
		&session.Source, &session.EntryCount, &session.TokenCount,
		&session.CreatedAt, &session.UpdatedAt,
	) {
		session.ID = uuid.UUID(idUUID).String()
		session.UserID = uuid.UUID(userIDUUIDResult).String()
		// Create a copy to append to the slice
		sessionCopy := session
		sessions = append(sessions, &sessionCopy)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to get diary sessions: %w", err)
	}

	return sessions, nil
}

// UpdateDiaryEntry updates a diary entry read model in Cassandra
func (r *diaryReadRepository) UpdateDiaryEntry(ctx context.Context, entry *models.DiaryEntryReadModel) error {
	query := `UPDATE diary_entries_read_model SET title = ?, content = ?, token_count = ?, 
		session_id = ?, tags = ?, deleted = ?, updated_at = ?, version = ? 
		WHERE id = ?`

	idUUID, err := stringToUUID(entry.ID)
	if err != nil {
		return fmt.Errorf("failed to convert entry ID to UUID: %w", err)
	}

	var sessionIDUUID gocql.UUID
	if entry.SessionID != "" {
		sessionIDUUID, err = stringToUUID(entry.SessionID)
		if err != nil {
			return fmt.Errorf("failed to convert session ID to UUID: %w", err)
		}
	}

	return r.session.Query(query,
		entry.Title, entry.Content, entry.TokenCount,
		sessionIDUUID, entry.Tags, entry.Deleted, entry.UpdatedAt, entry.Version, idUUID,
	).Exec()
}

// UpdateDiarySession updates a diary session read model in Cassandra
func (r *diaryReadRepository) UpdateDiarySession(ctx context.Context, session *models.DiarySessionReadModel) error {
	query := `UPDATE diary_sessions_read_model SET end_time = ?, entry_count = ?, 
		token_count = ?, updated_at = ? 
		WHERE id = ?`

	idUUID, err := stringToUUID(session.ID)
	if err != nil {
		return fmt.Errorf("failed to convert session ID to UUID: %w", err)
	}

	return r.session.Query(query,
		session.EndTime, session.EntryCount, session.TokenCount, session.UpdatedAt, idUUID,
	).Exec()
}

// DeleteDiaryEntry deletes a diary entry read model from Cassandra
func (r *diaryReadRepository) DeleteDiaryEntry(ctx context.Context, entryID string) error {
	// Get the entry to retrieve user ID and created time for index cleanup
	entry, err := r.GetDiaryEntryByID(ctx, entryID)
	if err != nil {
		return err
	}

	entryIDUUID, err := stringToUUID(entryID)
	if err != nil {
		return fmt.Errorf("failed to convert entry ID to UUID: %w", err)
	}

	userIDUUID, err := stringToUUID(entry.UserID)
	if err != nil {
		return fmt.Errorf("failed to convert user ID to UUID: %w", err)
	}

	// Delete from main table
	if err := r.session.Query(`DELETE FROM diary_entries_read_model WHERE id = ?`, entryIDUUID).Exec(); err != nil {
		return fmt.Errorf("failed to delete diary entry: %w", err)
	}

	// Delete from user index
	if err := r.session.Query(`DELETE FROM diary_entries_by_user_read_model WHERE user_id = ? AND entry_id = ?`, userIDUUID, entryIDUUID).Exec(); err != nil {
		return fmt.Errorf("failed to delete diary entry by user: %w", err)
	}

	// Delete from user and time index
	yearMonth := entry.CreatedAt.Format("2006-01")
	if err := r.session.Query(`DELETE FROM diary_entries_by_user_and_time_read_model WHERE user_id = ? AND year_month = ? AND entry_id = ?`, userIDUUID, yearMonth, entryIDUUID).Exec(); err != nil {
		return fmt.Errorf("failed to delete diary entry by user and time: %w", err)
	}

	return nil
}

// ProcessDiaryEvent processes a diary event and updates the read models accordingly
func (r *diaryReadRepository) ProcessDiaryEvent(ctx context.Context, event *events.Event) error {
	switch event.Type {
	case events.DiaryEntryCreatedEvent:
		return r.processDiaryEntryCreatedEvent(ctx, event)
	case events.DiaryEntryUpdatedEvent:
		return r.processDiaryEntryUpdatedEvent(ctx, event)
	case events.DiaryEntryDeletedEvent:
		return r.processDiaryEntryDeletedEvent(ctx, event)
	case events.DiarySessionStartedEvent:
		return r.processDiarySessionStartedEvent(ctx, event)
	case events.DiarySessionEndedEvent:
		return r.processDiarySessionEndedEvent(ctx, event)
	default:
		return fmt.Errorf("unsupported event type: %s", event.Type)
	}
}

// processDiaryEntryCreatedEvent processes a DiaryEntryCreated event
func (r *diaryReadRepository) processDiaryEntryCreatedEvent(ctx context.Context, event *events.Event) error {
	// Create diary entry read model
	entry, err := models.NewDiaryEntryReadModelFromEvent(event)
	if err != nil {
		return fmt.Errorf("failed to create diary entry read model: %w", err)
	}

	// Create diary entry by user read model
	entryByUser, err := models.NewDiaryEntryByUserReadModelFromEvent(event)
	if err != nil {
		return fmt.Errorf("failed to create diary entry by user read model: %w", err)
	}

	// Create diary entry by user and time read model
	entryByUserAndTime, err := models.NewDiaryEntryByUserAndTimeReadModelFromEvent(event)
	if err != nil {
		return fmt.Errorf("failed to create diary entry by user and time read model: %w", err)
	}

	// Save all read models
	if err := r.SaveDiaryEntry(ctx, entry); err != nil {
		return fmt.Errorf("failed to save diary entry read model: %w", err)
	}

	if err := r.SaveDiaryEntryByUser(ctx, entryByUser); err != nil {
		return fmt.Errorf("failed to save diary entry by user read model: %w", err)
	}

	if err := r.SaveDiaryEntryByUserAndTime(ctx, entryByUserAndTime); err != nil {
		return fmt.Errorf("failed to save diary entry by user and time read model: %w", err)
	}

	return nil
}

// processDiaryEntryUpdatedEvent processes a DiaryEntryUpdated event
func (r *diaryReadRepository) processDiaryEntryUpdatedEvent(ctx context.Context, event *events.Event) error {
	// Get existing entry
	entry, err := r.GetDiaryEntryByID(ctx, event.AggregateID)
	if err != nil {
		return fmt.Errorf("failed to get diary entry: %w", err)
	}

	// Update entry from event
	if err := entry.UpdateFromUpdateEvent(event); err != nil {
		return fmt.Errorf("failed to update diary entry from event: %w", err)
	}

	// Save updated entry
	if err := r.UpdateDiaryEntry(ctx, entry); err != nil {
		return fmt.Errorf("failed to update diary entry: %w", err)
	}

	return nil
}

// processDiaryEntryDeletedEvent processes a DiaryEntryDeleted event
func (r *diaryReadRepository) processDiaryEntryDeletedEvent(ctx context.Context, event *events.Event) error {
	// Get existing entry
	entry, err := r.GetDiaryEntryByID(ctx, event.AggregateID)
	if err != nil {
		return fmt.Errorf("failed to get diary entry: %w", err)
	}

	// Update entry from event
	if err := entry.UpdateFromDeleteEvent(event); err != nil {
		return fmt.Errorf("failed to update diary entry from event: %w", err)
	}

	// Save updated entry
	if err := r.UpdateDiaryEntry(ctx, entry); err != nil {
		return fmt.Errorf("failed to update diary entry: %w", err)
	}

	return nil
}

// processDiarySessionStartedEvent processes a DiarySessionStarted event
func (r *diaryReadRepository) processDiarySessionStartedEvent(ctx context.Context, event *events.Event) error {
	// Create diary session read model
	session, err := models.NewDiarySessionReadModelFromEvents(event, nil)
	if err != nil {
		return fmt.Errorf("failed to create diary session read model: %w", err)
	}

	// Save session
	if err := r.SaveDiarySession(ctx, session); err != nil {
		return fmt.Errorf("failed to save diary session read model: %w", err)
	}

	return nil
}

// processDiarySessionEndedEvent processes a DiarySessionEnded event
func (r *diaryReadRepository) processDiarySessionEndedEvent(ctx context.Context, event *events.Event) error {
	// Get existing session
	sessions, err := r.GetDiarySessionsByUserID(ctx, "")
	if err != nil {
		return fmt.Errorf("failed to get diary sessions: %w", err)
	}

	// Find the session with matching ID
	var session *models.DiarySessionReadModel
	for _, s := range sessions {
		if s.ID == event.AggregateID {
			session = s
			break
		}
	}

	if session == nil {
		return fmt.Errorf("session not found: %s", event.AggregateID)
	}

	// Update session from event
	if err := session.UpdateFromEndEvent(event); err != nil {
		return fmt.Errorf("failed to update diary session from event: %w", err)
	}

	// Save updated session
	if err := r.UpdateDiarySession(ctx, session); err != nil {
		return fmt.Errorf("failed to update diary session: %w", err)
	}

	return nil
}

// InitializeTables creates the necessary tables for diary read models in Cassandra
func (r *diaryReadRepository) InitializeTables() error {
	// Drop existing tables to recreate with correct schema (UUID)
	r.session.Query(`DROP TABLE IF EXISTS diary_entries_read_model`).Exec()
	r.session.Query(`DROP TABLE IF EXISTS diary_entries_by_user_read_model`).Exec()
	r.session.Query(`DROP TABLE IF EXISTS diary_entries_by_user_and_time_read_model`).Exec()
	r.session.Query(`DROP TABLE IF EXISTS diary_sessions_read_model`).Exec()

	// Create diary_entries_read_model table
	if err := r.session.Query(`CREATE TABLE IF NOT EXISTS diary_entries_read_model (
		id UUID PRIMARY KEY,
		user_id UUID,
		title TEXT,
		content TEXT,
		token_count INT,
		session_id UUID,
		tags LIST<TEXT>,
		deleted BOOLEAN,
		created_at TIMESTAMP,
		updated_at TIMESTAMP,
		version INT
	)`).Exec(); err != nil {
		return fmt.Errorf("failed to create diary_entries_read_model table: %w", err)
	}

	// Create diary_entries_by_user_read_model table
	if err := r.session.Query(`CREATE TABLE IF NOT EXISTS diary_entries_by_user_read_model (
		user_id UUID,
		entry_id UUID,
		title TEXT,
		created_at TIMESTAMP,
		token_count INT,
		tags LIST<TEXT>,
		deleted BOOLEAN,
		PRIMARY KEY (user_id, created_at, entry_id)
	) WITH CLUSTERING ORDER BY (created_at DESC, entry_id ASC)`).Exec(); err != nil {
		return fmt.Errorf("failed to create diary_entries_by_user_read_model table: %w", err)
	}

	// Create diary_entries_by_user_and_time_read_model table
	if err := r.session.Query(`CREATE TABLE IF NOT EXISTS diary_entries_by_user_and_time_read_model (
		user_id UUID,
		year_month TEXT,
		entry_id UUID,
		title TEXT,
		created_at TIMESTAMP,
		token_count INT,
		tags LIST<TEXT>,
		deleted BOOLEAN,
		PRIMARY KEY ((user_id, year_month), created_at, entry_id)
	) WITH CLUSTERING ORDER BY (created_at DESC, entry_id ASC)`).Exec(); err != nil {
		return fmt.Errorf("failed to create diary_entries_by_user_and_time_read_model table: %w", err)
	}

	// Create diary_sessions_read_model table
	if err := r.session.Query(`CREATE TABLE IF NOT EXISTS diary_sessions_read_model (
		id UUID PRIMARY KEY,
		user_id UUID,
		start_time TIMESTAMP,
		end_time TIMESTAMP,
		source TEXT,
		entry_count INT,
		token_count INT,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	)`).Exec(); err != nil {
		return fmt.Errorf("failed to create diary_sessions_read_model table: %w", err)
	}

	return nil
}
