package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all metrics for Diary Service
type Metrics struct {
	// HTTP metrics
	httpRequestsTotal   *prometheus.CounterVec
	httpRequestDuration *prometheus.HistogramVec
	httpResponseSize    *prometheus.HistogramVec

	// Business metrics
	entriesCreatedTotal  prometheus.Counter
	entriesUpdatedTotal  prometheus.Counter
	entriesDeletedTotal  prometheus.Counter
	sessionsStartedTotal prometheus.Counter
	sessionsEndedTotal   prometheus.Counter

	// Database metrics
	dbOperationsTotal   *prometheus.CounterVec
	dbOperationDuration *prometheus.HistogramVec

	// Kafka metrics
	kafkaMessagesTotal   *prometheus.CounterVec
	kafkaMessageDuration *prometheus.HistogramVec
}

// NewMetrics creates a new Metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		// HTTP metrics
		httpRequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "diary_service_http_requests_total",
				Help: "Total number of HTTP requests",
			},
			[]string{"method", "endpoint", "status_code"},
		),
		httpRequestDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "diary_service_http_request_duration_seconds",
				Help:    "Duration of HTTP requests in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method", "endpoint"},
		),
		httpResponseSize: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "diary_service_http_response_size_bytes",
				Help:    "Size of HTTP responses in bytes",
				Buckets: []float64{100, 1000, 10000, 100000, 1000000},
			},
			[]string{"method", "endpoint"},
		),

		// Business metrics
		entriesCreatedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diary_service_entries_created_total",
			Help: "Total number of diary entries created",
		}),
		entriesUpdatedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diary_service_entries_updated_total",
			Help: "Total number of diary entries updated",
		}),
		entriesDeletedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diary_service_entries_deleted_total",
			Help: "Total number of diary entries deleted",
		}),
		sessionsStartedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diary_service_sessions_started_total",
			Help: "Total number of diary sessions started",
		}),
		sessionsEndedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "diary_service_sessions_ended_total",
			Help: "Total number of diary sessions ended",
		}),

		// Database metrics
		dbOperationsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "diary_service_db_operations_total",
				Help: "Total number of database operations",
			},
			[]string{"operation", "table"},
		),
		dbOperationDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "diary_service_db_operation_duration_seconds",
				Help:    "Duration of database operations in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation", "table"},
		),

		// Kafka metrics
		kafkaMessagesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "diary_service_kafka_messages_total",
				Help: "Total number of Kafka messages",
			},
			[]string{"event_type", "status"},
		),
		kafkaMessageDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "diary_service_kafka_message_duration_seconds",
				Help:    "Duration of Kafka message processing in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"event_type"},
		),
	}
}

// ObserveHTTPRequest observes HTTP request metrics
func (m *Metrics) ObserveHTTPRequest(method, endpoint, statusCode string, duration time.Duration, size int64) {
	m.httpRequestsTotal.WithLabelValues(method, endpoint, statusCode).Inc()
	m.httpRequestDuration.WithLabelValues(method, endpoint).Observe(duration.Seconds())
	m.httpResponseSize.WithLabelValues(method, endpoint).Observe(float64(size))
}

// IncrementEntriesCreated increments the entries created counter
func (m *Metrics) IncrementEntriesCreated() {
	m.entriesCreatedTotal.Inc()
}

// IncrementEntriesUpdated increments the entries updated counter
func (m *Metrics) IncrementEntriesUpdated() {
	m.entriesUpdatedTotal.Inc()
}

// IncrementEntriesDeleted increments the entries deleted counter
func (m *Metrics) IncrementEntriesDeleted() {
	m.entriesDeletedTotal.Inc()
}

// IncrementSessionsStarted increments the sessions started counter
func (m *Metrics) IncrementSessionsStarted() {
	m.sessionsStartedTotal.Inc()
}

// IncrementSessionsEnded increments the sessions ended counter
func (m *Metrics) IncrementSessionsEnded() {
	m.sessionsEndedTotal.Inc()
}

// ObserveDBOperation observes database operation metrics
func (m *Metrics) ObserveDBOperation(operation, table string, duration time.Duration) {
	m.dbOperationsTotal.WithLabelValues(operation, table).Inc()
	m.dbOperationDuration.WithLabelValues(operation, table).Observe(duration.Seconds())
}

// ObserveKafkaMessage observes Kafka message metrics
func (m *Metrics) ObserveKafkaMessage(eventType, status string, duration time.Duration) {
	m.kafkaMessagesTotal.WithLabelValues(eventType, status).Inc()
	m.kafkaMessageDuration.WithLabelValues(eventType).Observe(duration.Seconds())
}

// HTTPMiddleware is a middleware that collects HTTP metrics
func (m *Metrics) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriter{w, http.StatusOK, 0}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)
		m.ObserveHTTPRequest(
			r.Method,
			r.URL.Path,
			strconv.Itoa(wrapped.status),
			duration,
			wrapped.size,
		)
	})
}

// responseWriter is a wrapper around http.ResponseWriter that captures status code and response size
type responseWriter struct {
	http.ResponseWriter
	status int
	size   int64
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.size += int64(n)
	return n, err
}
