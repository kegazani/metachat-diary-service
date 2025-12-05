#!/bin/bash

set -e

echo "🚀 Starting Metachat Diary Service Infrastructure..."

# Function to wait for a service to be healthy
wait_for_service() {
    local service_name=$1
    local max_attempts=${2:-30}
    local attempt=1
    
    echo "⏳ Waiting for $service_name to be healthy..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose -f config/docker-compose.yaml ps $service_name | grep -q "healthy\|Up"; then
            echo "✅ $service_name is healthy!"
            return 0
        fi
        
        echo "   Attempt $attempt/$max_attempts: $service_name not ready yet..."
        sleep 10
        ((attempt++))
    done
    
    echo "❌ $service_name failed to become healthy within expected time"
    return 1
}

# Function to check Cassandra connectivity
check_cassandra() {
    echo "🔍 Checking Cassandra connectivity..."
    
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if docker exec cassandra cqlsh -e "SELECT now() FROM system.local;" >/dev/null 2>&1; then
            echo "✅ Cassandra is ready for connections!"
            return 0
        fi
        
        echo "   Attempt $attempt/$max_attempts: Cassandra not ready yet..."
        sleep 10
        ((attempt++))
    done
    
    echo "❌ Cassandra failed to become ready within expected time"
    return 1
}

# Function to check Kafka connectivity
check_kafka() {
    echo "🔍 Checking Kafka connectivity..."
    
    local max_attempts=20
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
            echo "✅ Kafka is ready for connections!"
            return 0
        fi
        
        echo "   Attempt $attempt/$max_attempts: Kafka not ready yet..."
        sleep 10
        ((attempt++))
    done
    
    echo "❌ Kafka failed to become ready within expected time"
    return 1
}

# Start core infrastructure
echo "📦 Starting core infrastructure (Zookeeper, Kafka, Cassandra)..."
docker-compose -f config/docker-compose.yaml up -d zookeeper kafka cassandra

# Wait for services to be healthy
wait_for_service "zookeeper" 20
wait_for_service "kafka" 30
wait_for_service "cassandra" 40

# Additional connectivity checks
check_cassandra
check_kafka

# Start diary service
echo "📝 Starting diary service..."
docker-compose -f config/docker-compose.yaml up -d diary-service

# Wait for diary service to be healthy
wait_for_service "diary-service" 20

echo ""
echo "🎉 All services are up and running!"
echo ""
echo "📊 Service URLs:"
echo "   HTTP API:     http://localhost:8080"
echo "   gRPC API:     localhost:9093"
echo "   Health Check:  http://localhost:8080/health"
echo "   Metrics:      http://localhost:8080/metrics"
echo ""
echo "🔧 Management Commands:"
echo "   View logs:    docker-compose -f config/docker-compose.yaml logs -f diary-service"
echo "   Stop all:     docker-compose -f config/docker-compose.yaml down"
echo "   Restart:       docker-compose -f config/docker-compose.yaml restart diary-service"
echo ""
echo "📈 To start monitoring stack:"
echo "   docker-compose -f config/docker-compose.yaml --profile monitoring up -d"
echo ""
echo "📚 For more information, see README.md"