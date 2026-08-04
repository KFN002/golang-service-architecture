// Package auditstore adapts the audit PostgreSQL instance to audit.Store.
// It is INSERT-only by construction — no update or delete method exists.
package auditstore

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/internal/repo/auditstore/sqlcgen"
	"github.com/KFN002/perfect-go-service/internal/usecase/audit"
	"github.com/KFN002/perfect-go-service/pkg/jsonx"
)

// Store implements audit.Store over pgx.
type Store struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
}

// New builds the adapter.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, q: sqlcgen.New(pool)}
}

var _ audit.Store = (*Store)(nil)

// InsertBatch persists events idempotently via the unnest + ON CONFLICT
// group-commit statement.
func (s *Store) InsertBatch(ctx context.Context, events []entity.AuditEvent) (int64, error) {
	n := len(events)
	p := sqlcgen.InsertAuditEventsIdempotentParams{
		Column1: make([]uuid.UUID, n),
		Column2: make([]time.Time, n),
		Column3: make([]string, n),
		Column4: make([]string, n),
		Column5: make([]string, n),
		Column6: make([]string, n),
		Column7: make([]string, n),
		Column8: make([]string, n),
		Column9: make([][]byte, n),
	}
	for i, ev := range events {
		payload, err := jsonx.Marshal(ev.Payload)
		if err != nil {
			payload = []byte("{}")
		}
		p.Column1[i] = ev.ID
		p.Column2[i] = ev.OccurredAt.UTC()
		p.Column3[i] = string(ev.Type)
		p.Column4[i] = ev.Service
		p.Column5[i] = ev.EntityType
		p.Column6[i] = ev.EntityID
		p.Column7[i] = ev.TraceID
		p.Column8[i] = ev.Actor
		p.Column9[i] = payload
	}
	return s.q.InsertAuditEventsIdempotent(ctx, p)
}

// Query pages the log with keyset cursors.
func (s *Store) Query(ctx context.Context, f entity.AuditFilter) ([]entity.AuditEvent, error) {
	p := sqlcgen.QueryAuditEventsParams{PageSize: int32(f.Limit)}
	if !f.From.IsZero() {
		p.FromTs = &f.From
	}
	if !f.To.IsZero() {
		p.ToTs = &f.To
	}
	if f.Type != "" {
		t := string(f.Type)
		p.EventType = &t
	}
	if f.EntityType != "" {
		p.EntityType = &f.EntityType
	}
	if f.EntityID != "" {
		p.EntityID = &f.EntityID
	}
	if f.TraceID != "" {
		p.TraceID = &f.TraceID
	}
	if !f.CursorTime.IsZero() {
		p.CursorTs = &f.CursorTime
		p.CursorID = pgtype.UUID{Bytes: f.CursorID, Valid: true}
	}
	rows, err := s.q.QueryAuditEvents(ctx, p)
	if err != nil {
		return nil, err
	}
	out := make([]entity.AuditEvent, len(rows))
	for i, row := range rows {
		out[i] = toEvent(row)
	}
	return out, nil
}

// Stats aggregates counts.
func (s *Store) Stats(ctx context.Context) (entity.AuditStats, error) {
	total, err := s.q.CountAuditEvents(ctx)
	if err != nil {
		return entity.AuditStats{}, err
	}
	byType, err := s.q.AuditStatsByType(ctx)
	if err != nil {
		return entity.AuditStats{}, err
	}
	lastMin, err := s.q.AuditIngestLastMinute(ctx)
	if err != nil {
		return entity.AuditStats{}, err
	}
	stats := entity.AuditStats{Total: total, ByType: make(map[string]int64, len(byType)), Ingest1m: lastMin}
	for _, row := range byType {
		stats.ByType[row.EventType] = row.N
	}
	return stats, nil
}

// EnsurePartitions pre-creates daily partitions [today .. today+ahead].
func (s *Store) EnsurePartitions(ctx context.Context, ahead int) error {
	for i := 0; i <= ahead; i++ {
		if _, err := s.pool.Exec(ctx,
			"SELECT ensure_audit_partition(now()::date + $1)", i); err != nil {
			return fmt.Errorf("ensure partition +%d: %w", i, err)
		}
	}
	return nil
}

// Ping verifies the pool (readiness checks).
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func toEvent(row sqlcgen.AuditEvent) entity.AuditEvent {
	ev := entity.AuditEvent{
		ID:         row.ID,
		OccurredAt: row.OccurredAt,
		Type:       entity.AuditEventType(row.EventType),
		Service:    row.Service,
		EntityType: row.EntityType,
		EntityID:   row.EntityID,
		TraceID:    row.TraceID,
		Actor:      row.Actor,
	}
	_ = jsonx.Unmarshal(row.Payload, &ev.Payload)
	return ev
}
