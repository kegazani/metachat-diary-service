package api

import (
	"net/http"

	"github.com/gorilla/mux"

	"metachat/diary-service/internal/handlers"
)

// SetupRoutes sets up the routes for the API
func SetupRoutes(router *mux.Router, diaryHandler *handlers.DiaryHandler) {
	// Register diary routes
	diaryHandler.RegisterRoutes(router)

	// Add common middleware
	router.Use(commonMiddleware)
}

// commonMiddleware adds common middleware to all routes
func commonMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set common headers
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Call the next handler
		next.ServeHTTP(w, r)
	})
}
