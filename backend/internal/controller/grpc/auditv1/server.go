// Package auditv1 implements the AuditService gRPC server: bulkheaded,
// rate-limited synchronous ingestion plus the cached query side.
package auditv1

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	auditv1 "github.com/KFN002/perfect-go-service/gen/audit/v1"
	"github.com/KFN002/perfect-go-service/internal/controller/grpc/interceptors"
	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/internal/usecase/audit"
	"github.com/KFN002/perfect-go-service/pkg/apperrors"
	"github.com/KFN002/perfect-go-service/pkg/bulkhead"
	"github.com/KFN002/perfect-go-service/pkg/constants"
)

// Server serves AuditService.
type Server struct {
	auditv1.UnimplementedAuditServiceServer
	ingest *audit.Ingestor
	query  *audit.QueryService
	bh     *bulkhead.Bulkhead
}

// New builds the server; the bulkhead isolates the synchronous write path so
// a WriteEvents storm cannot starve queries or the AMQP ingesters.
func New(ingest *audit.Ingestor, query *audit.QueryService, writeCapacity int) *Server {
	return &Server{
		ingest: ingest,
		query:  query,
		bh:     bulkhead.New("grpc-write", writeCapacity),
	}
}

// WriteEvents ingests a batch synchronously (load-shed under saturation).
func (s *Server) WriteEvents(ctx context.Context, req *auditv1.WriteEventsRequest) (*auditv1.WriteEventsResponse, error) {
	events := req.GetEvents()
	if len(events) == 0 {
		return nil, interceptors.StatusFromError(apperrors.New(apperrors.CodeInvalidInput, "no events supplied"))
	}
	if len(events) > constants.MaxAuditBatch {
		return nil, interceptors.StatusFromError(apperrors.Newf(apperrors.CodeInvalidInput,
			"batch exceeds %d events", constants.MaxAuditBatch))
	}

	resp := &auditv1.WriteEventsResponse{}
	err := s.bh.Do(ctx, func(ctx context.Context) error {
		before := s.ingest.Stats()
		for _, pe := range events {
			ev, err := fromProto(pe)
			if err != nil {
				return err
			}
			if err := s.ingest.Ingest(ctx, ev); err != nil {
				return err
			}
		}
		after := s.ingest.Stats()
		// Deltas are bounded by MaxAuditBatch (≤1000) — safe narrowing.
		resp.Accepted = int32(min(after.Accepted-before.Accepted, int64(constants.MaxAuditBatch)))             // #nosec G115 -- min-clamped
		resp.Deduplicated = int32(min(after.Deduplicated-before.Deduplicated, int64(constants.MaxAuditBatch))) // #nosec G115 -- min-clamped
		return nil
	})
	if err != nil {
		return nil, interceptors.StatusFromError(err)
	}
	return resp, nil
}

// QueryEvents pages the log.
func (s *Server) QueryEvents(ctx context.Context, req *auditv1.QueryEventsRequest) (*auditv1.QueryEventsResponse, error) {
	f := entity.AuditFilter{
		Type:       entity.AuditEventType(req.GetEventType()),
		EntityType: req.GetEntityType(),
		EntityID:   req.GetEntityId(),
		TraceID:    req.GetTraceId(),
		Limit:      int(req.GetPageSize()),
	}
	if req.GetFrom() != nil {
		f.From = req.GetFrom().AsTime()
	}
	if req.GetTo() != nil {
		f.To = req.GetTo().AsTime()
	}
	if req.GetCursorTs() != nil && req.GetCursorId() != "" {
		id, err := uuid.Parse(req.GetCursorId())
		if err != nil {
			return nil, interceptors.StatusFromError(apperrors.New(apperrors.CodeInvalidInput, "cursor_id must be a UUID"))
		}
		f.CursorTime = req.GetCursorTs().AsTime()
		f.CursorID = id
	}

	events, err := s.query.Query(ctx, f)
	if err != nil {
		return nil, interceptors.StatusFromError(err)
	}

	resp := &auditv1.QueryEventsResponse{}
	for _, ev := range events {
		resp.Events = append(resp.Events, toProto(ev))
	}
	if len(events) > 0 {
		last := events[len(events)-1]
		resp.NextCursorTs = timestamppb.New(last.OccurredAt)
		resp.NextCursorId = last.ID.String()
	}
	return resp, nil
}

// GetStats returns aggregates.
func (s *Server) GetStats(ctx context.Context, _ *auditv1.GetStatsRequest) (*auditv1.AuditStats, error) {
	stats, err := s.query.Stats(ctx)
	if err != nil {
		return nil, interceptors.StatusFromError(err)
	}
	return &auditv1.AuditStats{
		Total:            stats.Total,
		ByType:           stats.ByType,
		IngestLastMinute: stats.Ingest1m,
	}, nil
}

// ---- mapping ---------------------------------------------------------------

func fromProto(pe *auditv1.AuditEvent) (entity.AuditEvent, error) {
	id, err := uuid.Parse(pe.GetId())
	if err != nil {
		return entity.AuditEvent{}, apperrors.New(apperrors.CodeInvalidInput, "event id must be a UUID")
	}
	ev := entity.AuditEvent{
		ID:         id,
		Type:       entity.AuditEventType(pe.GetEventType()),
		Service:    pe.GetService(),
		EntityType: pe.GetEntityType(),
		EntityID:   pe.GetEntityId(),
		TraceID:    pe.GetTraceId(),
		Actor:      pe.GetActor(),
	}
	if pe.GetOccurredAt() != nil {
		ev.OccurredAt = pe.GetOccurredAt().AsTime()
	} else {
		ev.OccurredAt = time.Now().UTC()
	}
	if pe.GetPayload() != nil {
		ev.Payload = pe.GetPayload().AsMap()
	}
	return ev, nil
}

func toProto(ev entity.AuditEvent) *auditv1.AuditEvent {
	out := &auditv1.AuditEvent{
		Id:         ev.ID.String(),
		OccurredAt: timestamppb.New(ev.OccurredAt),
		EventType:  string(ev.Type),
		Service:    ev.Service,
		EntityType: ev.EntityType,
		EntityId:   ev.EntityID,
		TraceId:    ev.TraceID,
		Actor:      ev.Actor,
	}
	if ev.Payload != nil {
		if st, err := structpb.NewStruct(ev.Payload); err == nil {
			out.Payload = st
		}
	}
	return out
}
