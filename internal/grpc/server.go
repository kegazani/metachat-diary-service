package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"metachat/diary-service/internal/models"
	"metachat/diary-service/internal/service"

	"github.com/kegazani/metachat-event-sourcing/aggregates"
	pb "github.com/kegazani/metachat-proto/diary"
)

// DiaryServer implements the gRPC DiaryService interface
type DiaryServer struct {
	pb.UnimplementedDiaryServiceServer
	diaryService service.DiaryService
	logger       *logrus.Logger
}

// NewDiaryServer creates a new diary gRPC server
func NewDiaryServer(diaryService service.DiaryService, logger *logrus.Logger) *DiaryServer {
	return &DiaryServer{
		diaryService: diaryService,
		logger:       logger,
	}
}

// CreateDiaryEntry creates a new diary entry
func (s *DiaryServer) CreateDiaryEntry(ctx context.Context, req *pb.CreateDiaryEntryRequest) (*pb.CreateDiaryEntryResponse, error) {
	correlationID := GetCorrelationID(ctx)
	s.logger.WithFields(logrus.Fields{
		"correlation_id": correlationID,
		"user_id":        req.UserId,
	}).Info("Creating diary entry via gRPC")

	entry, err := s.diaryService.CreateDiaryEntry(ctx, req.UserId, req.Title, req.Content, int(req.TokenCount), req.SessionId, req.Tags)
	if err != nil {
		s.logger.WithError(err).Error("Failed to create diary entry")
		return nil, fmt.Errorf("failed to create diary entry: %w", err)
	}

	return &pb.CreateDiaryEntryResponse{
		Entry: s.diaryEntryToProto(entry),
	}, nil
}

// GetDiaryEntry retrieves a diary entry by ID
func (s *DiaryServer) GetDiaryEntry(ctx context.Context, req *pb.GetDiaryEntryRequest) (*pb.GetDiaryEntryResponse, error) {
	s.logger.WithField("entry_id", req.Id).Info("Getting diary entry via gRPC")

	entry, err := s.diaryService.GetDiaryEntryByID(ctx, req.Id)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get diary entry")
		return nil, fmt.Errorf("failed to get diary entry: %w", err)
	}

	return &pb.GetDiaryEntryResponse{
		Entry: s.diaryEntryToProto(entry),
	}, nil
}

// UpdateDiaryEntry updates a diary entry
func (s *DiaryServer) UpdateDiaryEntry(ctx context.Context, req *pb.UpdateDiaryEntryRequest) (*pb.UpdateDiaryEntryResponse, error) {
	s.logger.WithField("entry_id", req.Id).Info("Updating diary entry via gRPC")

	entry, err := s.diaryService.UpdateDiaryEntry(ctx, req.Id, req.Title, req.Content, int(req.TokenCount), req.Tags)
	if err != nil {
		s.logger.WithError(err).Error("Failed to update diary entry")
		return nil, fmt.Errorf("failed to update diary entry: %w", err)
	}

	return &pb.UpdateDiaryEntryResponse{
		Entry: s.diaryEntryToProto(entry),
	}, nil
}

// DeleteDiaryEntry deletes a diary entry
func (s *DiaryServer) DeleteDiaryEntry(ctx context.Context, req *pb.DeleteDiaryEntryRequest) (*pb.DeleteDiaryEntryResponse, error) {
	s.logger.WithField("entry_id", req.Id).Info("Deleting diary entry via gRPC")

	err := s.diaryService.DeleteDiaryEntry(ctx, req.Id)
	if err != nil {
		s.logger.WithError(err).Error("Failed to delete diary entry")
		return nil, fmt.Errorf("failed to delete diary entry: %w", err)
	}

	return &pb.DeleteDiaryEntryResponse{
		Empty: &emptypb.Empty{},
	}, nil
}

// StartDiarySession starts a new diary session
func (s *DiaryServer) StartDiarySession(ctx context.Context, req *pb.StartDiarySessionRequest) (*pb.StartDiarySessionResponse, error) {
	s.logger.WithField("user_id", req.UserId).Info("Starting diary session via gRPC")

	// StartDiarySession is not implemented in the service interface yet
	// For now, return a mock session
	session := &aggregates.DiarySession{
		ID:          "session-" + req.UserId,
		UserID:      req.UserId,
		Title:       req.Title,
		Description: req.Description,
		StartedAt:   time.Now(),
		EndedAt:     time.Time{},
		Status:      "active",
		EntryCount:  0,
	}
	err := error(nil)
	if err != nil {
		s.logger.WithError(err).Error("Failed to start diary session")
		return nil, fmt.Errorf("failed to start diary session: %w", err)
	}

	return &pb.StartDiarySessionResponse{
		Session: s.diarySessionToProto(session),
	}, nil
}

// EndDiarySession ends a diary session
func (s *DiaryServer) EndDiarySession(ctx context.Context, req *pb.EndDiarySessionRequest) (*pb.EndDiarySessionResponse, error) {
	correlationID := GetCorrelationID(ctx)
	s.logger.WithFields(logrus.Fields{
		"correlation_id": correlationID,
		"session_id":     req.Id,
	}).Info("Ending diary session via gRPC")

	sessionReadModel, err := s.diaryService.GetDiarySessionReadModelByID(ctx, req.Id)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get diary session")
		return nil, fmt.Errorf("failed to get diary session: %w", err)
	}

	session := &aggregates.DiarySession{
		ID:          sessionReadModel.ID,
		UserID:      sessionReadModel.UserID,
		Title:       "",
		Description: "",
		StartedAt:   sessionReadModel.StartTime,
		EndedAt:     time.Now(),
		Status:      "ended",
		EntryCount:  sessionReadModel.EntryCount,
	}

	return &pb.EndDiarySessionResponse{
		Session: s.diarySessionToProto(session),
	}, nil
}

// GetDiarySession retrieves a diary session by ID
func (s *DiaryServer) GetDiarySession(ctx context.Context, req *pb.GetDiarySessionRequest) (*pb.GetDiarySessionResponse, error) {
	correlationID := GetCorrelationID(ctx)
	s.logger.WithFields(logrus.Fields{
		"correlation_id": correlationID,
		"session_id":     req.Id,
	}).Info("Getting diary session via gRPC")

	sessionReadModel, err := s.diaryService.GetDiarySessionReadModelByID(ctx, req.Id)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get diary session")
		return nil, fmt.Errorf("failed to get diary session: %w", err)
	}

	session := &aggregates.DiarySession{
		ID:          sessionReadModel.ID,
		UserID:      sessionReadModel.UserID,
		Title:       "",
		Description: "",
		StartedAt:   sessionReadModel.StartTime,
		EndedAt:     sessionReadModel.EndTime,
		Status:      "active",
		EntryCount:  sessionReadModel.EntryCount,
	}

	return &pb.GetDiarySessionResponse{
		Session: s.diarySessionToProto(session),
	}, nil
}

// GetDiaryEntryReadModel retrieves a diary entry read model
func (s *DiaryServer) GetDiaryEntryReadModel(ctx context.Context, req *pb.GetDiaryEntryReadModelRequest) (*pb.GetDiaryEntryReadModelResponse, error) {
	s.logger.WithField("entry_id", req.Id).Info("Getting diary entry read model via gRPC")

	entry, err := s.diaryService.GetDiaryEntryReadModelByID(ctx, req.Id)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get diary entry read model")
		return nil, fmt.Errorf("failed to get diary entry read model: %w", err)
	}

	return &pb.GetDiaryEntryReadModelResponse{
		Entry: s.diaryEntryReadModelToProto(entry),
	}, nil
}

// GetDiarySessionReadModel retrieves a diary session read model
func (s *DiaryServer) GetDiarySessionReadModel(ctx context.Context, req *pb.GetDiarySessionReadModelRequest) (*pb.GetDiarySessionReadModelResponse, error) {
	correlationID := GetCorrelationID(ctx)
	s.logger.WithFields(logrus.Fields{
		"correlation_id": correlationID,
		"session_id":     req.Id,
	}).Info("Getting diary session read model via gRPC")

	session, err := s.diaryService.GetDiarySessionReadModelByID(ctx, req.Id)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get diary session read model")
		return nil, fmt.Errorf("failed to get diary session read model: %w", err)
	}

	return &pb.GetDiarySessionReadModelResponse{
		Session: s.diarySessionReadModelToProto(session),
	}, nil
}

// ListDiaryEntries lists diary entries with pagination
func (s *DiaryServer) ListDiaryEntries(ctx context.Context, req *pb.ListDiaryEntriesRequest) (*pb.ListDiaryEntriesResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"page":   req.Page,
		"limit":  req.Limit,
		"filter": req.Filter,
	}).Info("Listing diary entries via gRPC")

	// ListDiaryEntries is not implemented in the service interface yet
	// For now, return empty list
	entries := []*aggregates.DiaryAggregate{}
	total := 0
	err := error(nil)
	if err != nil {
		s.logger.WithError(err).Error("Failed to list diary entries")
		return nil, fmt.Errorf("failed to list diary entries: %w", err)
	}

	protoEntries := make([]*pb.DiaryEntry, len(entries))
	for i, entry := range entries {
		protoEntries[i] = s.diaryEntryToProto(entry)
	}

	return &pb.ListDiaryEntriesResponse{
		Entries: protoEntries,
		Total:   int32(total),
		Page:    req.Page,
		Limit:   req.Limit,
	}, nil
}

// ListDiarySessions lists diary sessions with pagination
func (s *DiaryServer) ListDiarySessions(ctx context.Context, req *pb.ListDiarySessionsRequest) (*pb.ListDiarySessionsResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"page":   req.Page,
		"limit":  req.Limit,
		"filter": req.Filter,
	}).Info("Listing diary sessions via gRPC")

	// ListDiarySessions is not implemented in the service interface yet
	// For now, return empty list
	sessions := []*aggregates.DiarySession{}
	total := 0
	err := error(nil)
	if err != nil {
		s.logger.WithError(err).Error("Failed to list diary sessions")
		return nil, fmt.Errorf("failed to list diary sessions: %w", err)
	}

	protoSessions := make([]*pb.DiarySession, len(sessions))
	for i, session := range sessions {
		protoSessions[i] = s.diarySessionToProto(session)
	}

	return &pb.ListDiarySessionsResponse{
		Sessions: protoSessions,
		Total:    int32(total),
		Page:     req.Page,
		Limit:    req.Limit,
	}, nil
}

// GetDiaryEntriesByUser gets diary entries for a specific user
func (s *DiaryServer) GetDiaryEntriesByUser(ctx context.Context, req *pb.GetDiaryEntriesByUserRequest) (*pb.GetDiaryEntriesByUserResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"user_id": req.UserId,
		"page":    req.Page,
		"limit":   req.Limit,
	}).Info("Getting diary entries by user via gRPC")

	// GetDiaryEntriesByUser is not implemented in the service interface yet
	// For now, return empty list
	entries := []*aggregates.DiaryAggregate{}
	total := 0
	err := error(nil)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get diary entries by user")
		return nil, fmt.Errorf("failed to get diary entries by user: %w", err)
	}

	protoEntries := make([]*pb.DiaryEntry, len(entries))
	for i, entry := range entries {
		protoEntries[i] = s.diaryEntryToProto(entry)
	}

	return &pb.GetDiaryEntriesByUserResponse{
		Entries: protoEntries,
		Total:   int32(total),
		Page:    req.Page,
		Limit:   req.Limit,
	}, nil
}

// GetDiarySessionsByUser gets diary sessions for a specific user
func (s *DiaryServer) GetDiarySessionsByUser(ctx context.Context, req *pb.GetDiarySessionsByUserRequest) (*pb.GetDiarySessionsByUserResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"user_id": req.UserId,
		"page":    req.Page,
		"limit":   req.Limit,
	}).Info("Getting diary sessions by user via gRPC")

	// GetDiarySessionsByUser is not implemented in the service interface yet
	// For now, return empty list
	sessions := []*aggregates.DiarySession{}
	total := 0
	err := error(nil)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get diary sessions by user")
		return nil, fmt.Errorf("failed to get diary sessions by user: %w", err)
	}

	protoSessions := make([]*pb.DiarySession, len(sessions))
	for i, session := range sessions {
		protoSessions[i] = s.diarySessionToProto(session)
	}

	return &pb.GetDiarySessionsByUserResponse{
		Sessions: protoSessions,
		Total:    int32(total),
		Page:     req.Page,
		Limit:    req.Limit,
	}, nil
}

// GetDiaryAnalytics retrieves diary analytics
func (s *DiaryServer) GetDiaryAnalytics(ctx context.Context, req *pb.GetDiaryAnalyticsRequest) (*pb.GetDiaryAnalyticsResponse, error) {
	s.logger.Info("Getting diary analytics via gRPC")

	// GetDiaryAnalytics is not implemented in the service interface yet
	// For now, return mock analytics
	analytics := &aggregates.DiaryAnalytics{
		TotalEntries:             0,
		TotalSessions:            0,
		ActiveUsers:              0,
		AverageEntriesPerSession: 0,
		AverageSessionsPerUser:   0,
		LastUpdated:              time.Now(),
	}
	err := error(nil)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get diary analytics")
		return nil, fmt.Errorf("failed to get diary analytics: %w", err)
	}

	return &pb.GetDiaryAnalyticsResponse{
		Analytics: s.diaryAnalyticsToProto(analytics),
	}, nil
}

// Helper methods to convert domain models to protobuf messages
func (s *DiaryServer) diaryEntryToProto(entry *aggregates.DiaryAggregate) *pb.DiaryEntry {
	return &pb.DiaryEntry{
		Id:         entry.GetID(),
		UserId:     entry.GetUserID(),
		Title:      entry.GetTitle(),
		Content:    entry.GetContent(),
		TokenCount: int32(entry.GetTokenCount()),
		SessionId:  entry.GetSessionID(),
		Tags:       entry.GetTags(),
		CreatedAt:  timestamppb.New(entry.GetCreatedAt()),
		UpdatedAt:  timestamppb.New(entry.GetUpdatedAt()),
	}
}

func (s *DiaryServer) diarySessionToProto(session *aggregates.DiarySession) *pb.DiarySession {
	return &pb.DiarySession{
		Id:          session.ID,
		UserId:      session.UserID,
		Title:       session.Title,
		Description: session.Description,
		StartedAt:   timestamppb.New(session.StartedAt),
		EndedAt:     timestamppb.New(session.EndedAt),
		Status:      session.Status,
		EntryCount:  int32(session.EntryCount),
	}
}

func (s *DiaryServer) diaryEntryReadModelToProto(entry *models.DiaryEntryReadModel) *pb.DiaryEntryReadModel {
	return &pb.DiaryEntryReadModel{
		Id:         entry.ID,
		UserId:     entry.UserID,
		Title:      entry.Title,
		Content:    entry.Content,
		TokenCount: int32(entry.TokenCount),
		SessionId:  entry.SessionID,
		Tags:       entry.Tags,
		CreatedAt:  timestamppb.New(entry.CreatedAt),
		UpdatedAt:  timestamppb.New(entry.UpdatedAt),
	}
}

func (s *DiaryServer) diarySessionReadModelToProto(session *models.DiarySessionReadModel) *pb.DiarySessionReadModel {
	return &pb.DiarySessionReadModel{
		Id:          session.ID,
		UserId:      session.UserID,
		Title:       "", // Not available in DiarySessionReadModel
		Description: "", // Not available in DiarySessionReadModel
		StartedAt:   timestamppb.New(session.StartTime),
		EndedAt:     timestamppb.New(session.EndTime),
		Status:      "", // Not available in DiarySessionReadModel
		EntryCount:  int32(session.EntryCount),
		CreatedAt:   timestamppb.New(session.CreatedAt),
		UpdatedAt:   timestamppb.New(session.UpdatedAt),
	}
}

func (s *DiaryServer) diaryAnalyticsToProto(analytics *aggregates.DiaryAnalytics) *pb.DiaryAnalytics {
	return &pb.DiaryAnalytics{
		TotalEntries:             analytics.TotalEntries,
		TotalSessions:            analytics.TotalSessions,
		ActiveUsers:              analytics.ActiveUsers,
		AverageEntriesPerSession: analytics.AverageEntriesPerSession,
		AverageSessionsPerUser:   analytics.AverageSessionsPerUser,
		LastUpdated:              timestamppb.New(analytics.LastUpdated),
	}
}

// StartGRPCServer starts the gRPC server
func StartGRPCServer(diaryService service.DiaryService, logger *logrus.Logger, port string) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	// Configure gRPC server with reasonable defaults
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(4 * 1024 * 1024), // 4MB
		grpc.MaxSendMsgSize(4 * 1024 * 1024), // 4MB
		grpc.UnaryInterceptor(CorrelationIDInterceptor(logger)),
	}

	s := grpc.NewServer(opts...)
	pb.RegisterDiaryServiceServer(s, NewDiaryServer(diaryService, logger))

	// Enable reflection for development
	reflection.Register(s)

	logger.WithField("port", port).Info("Starting gRPC server")

	return s.Serve(lis)
}
