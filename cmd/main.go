package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"

	"github.com/metachat/common/event-sourcing/aggregates"
	"github.com/metachat/common/event-sourcing/serializer"
	"github.com/metachat/common/event-sourcing/store"
	"github.com/metachat/config/logging"
	"metachat/diary-service/internal/api"
	"metachat/diary-service/internal/handlers"
	"metachat/diary-service/internal/kafka"
	"metachat/diary-service/internal/repository"
	"metachat/diary-service/internal/service"
)

func main() {
	// Initialize logger
	loggerConfig := logging.LoggerConfig{
		ServiceName: "diary-service",
		Environment: viper.GetString("environment"),
		LogLevel:    viper.GetString("log.level"),
		LogFormat:   viper.GetString("log.format"),
		LogOutput:   viper.GetString("log.output"),
	}
	logger := logging.NewLogger(loggerConfig)

	// Load configuration
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./config")
	viper.AddConfigPath("/app/config")

	if err := viper.ReadInConfig(); err != nil {
		logger.Fatalf("Failed to read config file: %v", err)
	}

	// Initialize event store
	var eventStore store.EventStore
	eventStoreType := viper.GetString("event_store.type")

	switch eventStoreType {
	case "memory":
		eventStore = store.NewMemoryEventStore()
	case "eventstoredb":
		// TODO: Implement EventStoreDB client
		logger.Fatal("EventStoreDB client not implemented yet")
	default:
		eventStore = store.NewMemoryEventStore()
		logger.Warn("Using in-memory event store (not suitable for production)")
	}

	// Initialize serializer
	serializer := serializer.NewJSONSerializer()

	// Initialize Cassandra cluster
	cluster := gocql.NewCluster(viper.GetString("cassandra.hosts"))
	cluster.Keyspace = viper.GetString("cassandra.keyspace")
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second

	// Create Cassandra session
	session, err := cluster.CreateSession()
	if err != nil {
		logger.Fatalf("Failed to connect to Cassandra: %v", err)
	}
	defer session.Close()

	// Initialize repositories
	diaryRepository := repository.NewDiaryRepository(eventStore, serializer)
	diaryReadRepository := repository.NewDiaryReadRepository(session)

	// Initialize Cassandra tables
	if err := diaryReadRepository.InitializeTables(); err != nil {
		logger.Fatalf("Failed to initialize Cassandra tables: %v", err)
	}

	// Initialize Kafka producer
	kafkaBootstrapServers := viper.GetString("kafka.bootstrap_servers")
	kafkaTopic := viper.GetString("kafka.diary_events_topic")
	diaryEventProducer, err := kafka.NewDiaryEventProducer(kafkaBootstrapServers, kafkaTopic)
	if err != nil {
		logger.Fatalf("Failed to create diary event producer: %v", err)
	}
	defer diaryEventProducer.Close()

	// Initialize services
	diaryService := service.NewDiaryService(diaryRepository, diaryReadRepository, diaryEventProducer)

	// Initialize aggregates
	diaryAggregateFactory := func(id string) aggregates.Aggregate {
		return aggregates.NewDiaryAggregate(id)
	}

	// Initialize handlers
	diaryHandler := handlers.NewDiaryHandler(diaryService, diaryAggregateFactory, eventStore, serializer)

	// Initialize API
	router := mux.NewRouter()
	api.SetupRoutes(router, diaryHandler)

	// Create HTTP server
	port := viper.GetString("server.port")
	if port == "" {
		port = "8080"
	}

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		logger.Infof("Starting server on port %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")

	// Create context with timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Errorf("Server forced to shutdown: %v", err)
	}

	logger.Info("Server exited")
}
