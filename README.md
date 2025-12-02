# Metachat Diary Service

A microservice for managing diary entries and sessions with event sourcing architecture.

## Architecture

This service uses:
- **Event Sourcing** with CQRS pattern
- **Apache Cassandra** for data persistence
- **Apache Kafka** for event streaming
- **NATS** as alternative messaging system
- **gRPC** for internal communication
- **HTTP REST** for external APIs
- **Prometheus** for metrics collection
- **Grafana** for visualization

## Quick Start with Docker Compose

### Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available

### Basic Infrastructure

Start the core infrastructure (Zookeeper, Kafka, Cassandra):

```bash
docker-compose -f config/docker-compose.yaml up -d zookeeper kafka cassandra
```

Wait for all services to be healthy (check with `docker-compose ps`).

### Start the Diary Service

```bash
docker-compose -f config/docker-compose.yaml up -d diary-service
```

### Full Stack with Monitoring

Start all services including monitoring:

```bash
docker-compose -f config/docker-compose.yaml --profile monitoring up -d
```

This will start:
- Core infrastructure (Zookeeper, Kafka, Cassandra)
- Diary Service
- NATS (alternative messaging)
- Prometheus (metrics collection)
- Grafana (visualization)

## Service Endpoints

### HTTP API
- Base URL: `http://localhost:8080`
- Health Check: `http://localhost:8080/health`
- Metrics: `http://localhost:8080/metrics`

### gRPC API
- Address: `localhost:9093`

### Monitoring
- Prometheus: `http://localhost:9091`
- Grafana: `http://localhost:3000` (admin/admin)

## Configuration

The service can be configured through environment variables or the `config/config.yaml` file.

### Key Configuration Options

| Variable | Default | Description |
|----------|---------|-------------|
| `HTTP_PORT` | 8080 | HTTP server port |
| `GRPC_PORT` | 9093 | gRPC server port |
| `CASSANDRA_HOSTS` | localhost | Cassandra hosts |
| `KAFKA_BROKERS` | localhost:9092 | Kafka brokers |
| `LOG_LEVEL` | info | Logging level |

## Development

### Local Development

1. Install dependencies:
```bash
go mod download
```

2. Start infrastructure:
```bash
docker-compose -f config/docker-compose.yaml up -d zookeeper kafka cassandra
```

3. Run the service:
```bash
go run cmd/main.go
```

### Running Tests

```bash
go test ./...
```

### Building

```bash
go build -o diary-service cmd/main.go
```

## API Documentation

### gRPC Methods

#### Diary Entries
- `CreateDiaryEntry` - Create a new diary entry
- `GetDiaryEntry` - Retrieve a diary entry by ID
- `UpdateDiaryEntry` - Update an existing diary entry
- `DeleteDiaryEntry` - Delete a diary entry
- `ListDiaryEntries` - List diary entries with pagination
- `GetDiaryEntriesByUser` - Get diary entries for a specific user

#### Diary Sessions
- `StartDiarySession` - Start a new diary session
- `EndDiarySession` - End an active diary session
- `GetDiarySession` - Retrieve a diary session by ID
- `ListDiarySessions` - List diary sessions with pagination
- `GetDiarySessionsByUser` - Get diary sessions for a specific user

#### Read Models
- `GetDiaryEntryReadModel` - Get optimized read model for diary entry
- `GetDiarySessionReadModel` - Get optimized read model for diary session

#### Analytics
- `GetDiaryAnalytics` - Get analytics data for the diary service

### HTTP REST API

The service also exposes a REST API for external clients:

#### Diary Entries
- `GET /api/v1/entries` - List entries
- `POST /api/v1/entries` - Create entry
- `GET /api/v1/entries/{id}` - Get entry
- `PUT /api/v1/entries/{id}` - Update entry
- `DELETE /api/v1/entries/{id}` - Delete entry

#### Diary Sessions
- `GET /api/v1/sessions` - List sessions
- `POST /api/v1/sessions` - Create session
- `GET /api/v1/sessions/{id}` - Get session
- `PUT /api/v1/sessions/{id}/end` - End session

## Monitoring

### Metrics

The service exposes Prometheus metrics at `/metrics` including:
- HTTP request metrics (count, duration)
- gRPC request metrics
- Database operation metrics
- Kafka/NATS message metrics
- Custom business metrics

### Health Checks

- `GET /health` - Overall service health
- `GET /health/ready` - Readiness probe
- `GET /health/live` - Liveness probe

## Troubleshooting

### Common Issues

1. **Cassandra connection failed**
   - Ensure Cassandra is fully started (may take 2-3 minutes)
   - Check keyspace exists: `docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES"`

2. **Kafka connection failed**
   - Verify Kafka is healthy: `docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092`
   - Check topics exist: `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list`

3. **Service won't start**
   - Check logs: `docker-compose logs diary-service`
   - Verify all dependencies are healthy: `docker-compose ps`

### Logs

View service logs:
```bash
docker-compose logs -f diary-service
```

View all logs:
```bash
docker-compose logs -f
```

## Production Deployment

### Environment Variables

For production, ensure these are properly set:

```bash
# Security
JWT_SECRET=your-secure-jwt-secret
JWT_EXPIRATION=24h

# Database
CASSANDRA_USERNAME=cassandra
CASSANDRA_PASSWORD=secure-password
CASSANDRA_CONSISTENCY=QUORUM

# Kafka
KAFKA_SASL_USERNAME=kafka-user
KAFKA_SASL_PASSWORD=kafka-password
KAFKA_SECURITY_PROTOCOL=SASL_SSL
```

### Resource Limits

Configure appropriate resource limits in production:

```yaml
services:
  diary-service:
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 1G
        reservations:
          cpus: '0.5'
          memory: 512M
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

[Your License Here]