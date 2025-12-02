package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"

	"metachat/diary-service/internal/api"
	"metachat/diary-service/internal/grpc"
	"metachat/diary-service/internal/handlers"
	"metachat/diary-service/internal/kafka"
	"metachat/diary-service/internal/metrics"
	"metachat/diary-service/internal/repository"
	"metachat/diary-service/internal/service"

	"github.com/kegazani/metachat-event-sourcing/aggregates"
	"github.com/kegazani/metachat-event-sourcing/serializer"
	"github.com/kegazani/metachat-event-sourcing/store"
	"github.com/sirupsen/logrus"
)

func main() {
	// Load configuration
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./config")
	viper.AddConfigPath("/app/config")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	// Initialize logger with config
	logger := logrus.New()

	// Configure logging from config
	logLevel := viper.GetString("logging.level")
	switch logLevel {
	case "debug":
		logger.SetLevel(logrus.DebugLevel)
	case "info":
		logger.SetLevel(logrus.InfoLevel)
	case "warn":
		logger.SetLevel(logrus.WarnLevel)
	case "error":
		logger.SetLevel(logrus.ErrorLevel)
	default:
		logger.SetLevel(logrus.InfoLevel)
	}

	logFormat := viper.GetString("logging.format")
	if logFormat == "json" {
		logger.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logger.SetFormatter(&logrus.TextFormatter{})
	}

	logger.WithFields(logrus.Fields{
		"service": viper.GetString("service.name"),
		"version": viper.GetString("service.version"),
		"env":     viper.GetString("service.environment"),
	}).Info("Starting diary service")

	// Initialize event store
	var eventStore store.EventStore
	eventStoreType := viper.GetString("event_store.type")

	switch eventStoreType {
	case "memory":
		eventStore = store.NewMemoryEventStore()
		logger.Info("Using in-memory event store")
	case "eventstoredb":
		// TODO: Implement EventStoreDB client
		logger.Fatal("EventStoreDB client not implemented yet")
	default:
		eventStore = store.NewMemoryEventStore()
		logger.Warn("Using in-memory event store (not suitable for production)")
	}

	// Initialize serializer
	serializer := serializer.NewJSONSerializer()

	// Initialize Cassandra connection
	hosts := viper.GetStringSlice("cassandra.hosts")
	if len(hosts) == 0 {
		if h := viper.GetString("cassandra.hosts"); h != "" {
			hosts = []string{h}
		}
	}

	var session *gocql.Session
	var err error

	// Parse timeout from config
	timeout, err := time.ParseDuration(viper.GetString("cassandra.timeout"))
	if err != nil {
		timeout = 10 * time.Second
		logger.Warnf("Invalid Cassandra timeout, using default: %v", timeout)
	}

	// Parse reconnect interval from config
	reconnectInterval, err := time.ParseDuration(viper.GetString("cassandra.reconnect_interval"))
	if err != nil {
		reconnectInterval = 10 * time.Second
		logger.Warnf("Invalid Cassandra reconnect interval, using default: %v", reconnectInterval)
	}

	// Parse consistency level
	consistency := gocql.Quorum
	switch viper.GetString("cassandra.consistency") {
	case "ONE":
		consistency = gocql.One
	case "QUORUM":
		consistency = gocql.Quorum
	case "ALL":
		consistency = gocql.All
	case "LOCAL_QUORUM":
		consistency = gocql.LocalQuorum
	}

	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		cluster := gocql.NewCluster(hosts...)
		cluster.Keyspace = viper.GetString("cassandra.keyspace")
		cluster.Consistency = consistency
		cluster.Timeout = timeout
		cluster.NumConns = viper.GetInt("cassandra.num_connections")

		// Set authentication if provided
		if username := viper.GetString("cassandra.username"); username != "" {
			cluster.Authenticator = gocql.PasswordAuthenticator{
				Username: username,
				Password: viper.GetString("cassandra.password"),
			}
		}

		session, err = cluster.CreateSession()
		if err == nil {
			logger.Info("Successfully connected to Cassandra")
			break
		}
		logger.WithError(err).Warnf("Failed to connect to Cassandra (attempt %d/%d), retrying...", i+1, maxRetries)
		time.Sleep(reconnectInterval)
	}

	if err != nil {
		logger.Fatalf("Failed to connect to Cassandra after %d attempts: %v", maxRetries, err)
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
	kafkaBrokers := viper.GetStringSlice("kafka.brokers")
	kafkaTopic := viper.GetString("kafka.diary_events_topic")
	diaryEventProducer, err := kafka.NewDiaryEventProducer(strings.Join(kafkaBrokers, ","), kafkaTopic)
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

	// Initialize metrics if enabled
	if viper.GetBool("metrics.enabled") {
		metricsInstance := metrics.NewMetrics()
		router.Use(metricsInstance.HTTPMiddleware)

		// Setup metrics endpoint
		router.Path(viper.GetString("metrics.path")).Handler(promhttp.Handler())

		// Start metrics server in a goroutine
		go func() {
			metricsPort := viper.GetString("metrics.port")
			metricsRouter := mux.NewRouter()
			metricsRouter.Path(viper.GetString("metrics.path")).Handler(promhttp.Handler())

			metricsSrv := &http.Server{
				Addr:    ":" + metricsPort,
				Handler: metricsRouter,
			}

			logger.WithField("port", metricsPort).Info("Starting metrics server")
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.WithError(err).Error("Failed to start metrics server")
			}
		}()
	}

	// Parse server timeouts from config
	readTimeout, _ := time.ParseDuration(viper.GetString("server.read_timeout"))
	writeTimeout, _ := time.ParseDuration(viper.GetString("server.write_timeout"))
	idleTimeout, _ := time.ParseDuration(viper.GetString("server.idle_timeout"))

	// Create HTTP server
	srv := &http.Server{
		Addr:         viper.GetString("server.host") + ":" + viper.GetString("server.port"),
		Handler:      router,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	// Start gRPC server in a goroutine if configured
	go func() {
		grpcPort := viper.GetString("grpc.port")
		logger.WithField("port", grpcPort).Info("Starting gRPC server")

		if err := grpc.StartGRPCServer(diaryService, logger, grpcPort); err != nil {
			logger.WithError(err).Error("Failed to start gRPC server")
		}
	}()

	// Start HTTP server in a goroutine
	go func() {
		logger.WithFields(logrus.Fields{
			"host": viper.GetString("server.host"),
			"port": viper.GetString("server.port"),
		}).Info("Starting HTTP server")

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
