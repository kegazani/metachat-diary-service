package grpc

import (
	"context"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const CorrelationIDMetadataKey = "correlation_id"

type correlationIDKey struct{}

var CorrelationIDKey = correlationIDKey{}

func CorrelationIDInterceptor(logger *logrus.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		correlationID := extractCorrelationID(ctx)
		if correlationID == "" {
			correlationID = uuid.New().String()
		}

		ctx = context.WithValue(ctx, CorrelationIDKey, correlationID)
		logger.WithField("correlation_id", correlationID).WithField("method", info.FullMethod).Info("gRPC request received")

		return handler(ctx, req)
	}
}

func extractCorrelationID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	values := md.Get(CorrelationIDMetadataKey)
	if len(values) > 0 {
		return values[0]
	}

	return ""
}

func GetCorrelationID(ctx context.Context) string {
	if correlationID, ok := ctx.Value(CorrelationIDKey).(string); ok {
		return correlationID
	}
	return ""
}
