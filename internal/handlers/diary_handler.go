package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/metachat/common/event-sourcing/aggregates"
	"github.com/metachat/common/event-sourcing/serializer"
	"github.com/metachat/common/event-sourcing/store"
	"metachat/diary-service/internal/service"
)

// DiaryHandler handles HTTP requests for diary operations
type DiaryHandler struct {
	diaryService          service.DiaryService
	diaryAggregateFactory func(string) aggregates.Aggregate
	eventStore            store.EventStore
	serializer            serializer.Serializer
	logger                *logrus.Logger
}

// NewDiaryHandler creates a new diary handler
func NewDiaryHandler(
	diaryService service.DiaryService,
	diaryAggregateFactory func(string) aggregates.Aggregate,
	eventStore store.EventStore,
	serializer serializer.Serializer,
) *DiaryHandler {
	return &DiaryHandler{
		diaryService:          diaryService,
		diaryAggregateFactory: diaryAggregateFactory,
		eventStore:            eventStore,
		serializer:            serializer,
		logger:                logrus.New(),
	}
}

// RegisterRoutes registers the routes for the diary handler
func (h *DiaryHandler) RegisterRoutes(router *mux.Router) {
	// Diary routes
	router.HandleFunc("/diary-entries", h.CreateDiaryEntry).Methods("POST")
	router.HandleFunc("/diary-entries/{id}", h.GetDiaryEntry).Methods("GET")
	router.HandleFunc("/diary-entries/{id}", h.UpdateDiaryEntry).Methods("PUT")
	router.HandleFunc("/diary-entries/{id}", h.DeleteDiaryEntry).Methods("DELETE")
	router.HandleFunc("/users/{id}/diary-entries", h.GetDiaryEntriesByUserID).Methods("GET")
	router.HandleFunc("/users/{id}/diary-entries/range", h.GetDiaryEntriesByUserIDAndTimeRange).Methods("GET")
}

// CreateDiaryEntry handles the creation of a new diary entry
func (h *DiaryHandler) CreateDiaryEntry(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req struct {
		UserID     string   `json:"user_id"`
		Title      string   `json:"title"`
		Content    string   `json:"content"`
		TokenCount int      `json:"token_count"`
		SessionID  string   `json:"session_id,omitempty"`
		Tags       []string `json:"tags,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.WithError(err).Error("Failed to decode request body")
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	diary, err := h.diaryService.CreateDiaryEntry(ctx, req.UserID, req.Title, req.Content, req.TokenCount, req.SessionID, req.Tags)
	if err != nil {
		h.logger.WithError(err).Error("Failed to create diary entry")
		http.Error(w, "Failed to create diary entry", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(h.diaryToResponse(diary))
}

// GetDiaryEntry handles retrieving a diary entry by ID
func (h *DiaryHandler) GetDiaryEntry(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	diaryID := vars["id"]

	diary, err := h.diaryService.GetDiaryEntryByID(ctx, diaryID)
	if err != nil {
		h.logger.WithError(err).Error("Failed to get diary entry")
		if err == store.ErrEventNotFound {
			http.Error(w, "Diary entry not found", http.StatusNotFound)
		} else {
			http.Error(w, "Failed to get diary entry", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(h.diaryToResponse(diary))
}

// UpdateDiaryEntry handles updating a diary entry
func (h *DiaryHandler) UpdateDiaryEntry(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	diaryID := vars["id"]

	var req struct {
		Title      string   `json:"title"`
		Content    string   `json:"content"`
		TokenCount int      `json:"token_count"`
		Tags       []string `json:"tags,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.WithError(err).Error("Failed to decode request body")
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	diary, err := h.diaryService.UpdateDiaryEntry(ctx, diaryID, req.Title, req.Content, req.TokenCount, req.Tags)
	if err != nil {
		h.logger.WithError(err).Error("Failed to update diary entry")
		if err == store.ErrEventNotFound {
			http.Error(w, "Diary entry not found", http.StatusNotFound)
		} else {
			http.Error(w, "Failed to update diary entry", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(h.diaryToResponse(diary))
}

// DeleteDiaryEntry handles deleting a diary entry
func (h *DiaryHandler) DeleteDiaryEntry(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	diaryID := vars["id"]

	if err := h.diaryService.DeleteDiaryEntry(ctx, diaryID); err != nil {
		h.logger.WithError(err).Error("Failed to delete diary entry")
		if err == store.ErrEventNotFound {
			http.Error(w, "Diary entry not found", http.StatusNotFound)
		} else {
			http.Error(w, "Failed to delete diary entry", http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GetDiaryEntriesByUserID handles retrieving all diary entries for a user
func (h *DiaryHandler) GetDiaryEntriesByUserID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	userID := vars["id"]

	diaries, err := h.diaryService.GetDiaryEntriesByUserID(ctx, userID)
	if err != nil {
		h.logger.WithError(err).Error("Failed to get diary entries")
		http.Error(w, "Failed to get diary entries", http.StatusInternalServerError)
		return
	}

	response := make([]map[string]interface{}, 0, len(diaries))
	for _, diary := range diaries {
		response = append(response, h.diaryToResponse(diary))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// GetDiaryEntriesByUserIDAndTimeRange handles retrieving diary entries for a user within a time range
func (h *DiaryHandler) GetDiaryEntriesByUserIDAndTimeRange(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	userID := vars["id"]

	query := r.URL.Query()
	startTime := query.Get("start_time")
	endTime := query.Get("end_time")

	if startTime == "" || endTime == "" {
		http.Error(w, "start_time and end_time query parameters are required", http.StatusBadRequest)
		return
	}

	diaries, err := h.diaryService.GetDiaryEntriesByUserIDAndTimeRange(ctx, userID, startTime, endTime)
	if err != nil {
		h.logger.WithError(err).Error("Failed to get diary entries")
		http.Error(w, "Failed to get diary entries", http.StatusInternalServerError)
		return
	}

	response := make([]map[string]interface{}, 0, len(diaries))
	for _, diary := range diaries {
		response = append(response, h.diaryToResponse(diary))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// diaryToResponse converts a diary aggregate to a response object
func (h *DiaryHandler) diaryToResponse(diary *aggregates.DiaryAggregate) map[string]interface{} {
	return map[string]interface{}{
		"id":          diary.GetID(),
		"user_id":     diary.GetUserID(),
		"title":       diary.GetTitle(),
		"content":     diary.GetContent(),
		"token_count": diary.GetTokenCount(),
		"session_id":  diary.GetSessionID(),
		"tags":        diary.GetTags(),
		"deleted":     diary.IsDeleted(),
	}
}
