// Package audit implements the append-only audit-log usecases.
//
// Write path (per event):
//
//	decode → validate → dedup fast-path → bounded ingress (backpressure) →
//	micro-batcher (size/interval, double-buffered) → idempotent group commit.
//
// Each event carries a completion channel: the AMQP ack (or gRPC response)
// waits for its batch's flush, so durability is end-to-end — an unflushed
// event is an unacked message, and the broker redelivers it.
package audit

import (
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
	"github.com/KFN002/perfect-go-service/pkg/apperrors"
	"github.com/KFN002/perfect-go-service/pkg/batcher"
	"github.com/KFN002/perfect-go-service/pkg/constants"
	"github.com/KFN002/perfect-go-service/pkg/jsonx"
)

// IngestConfig tunes the write pipeline.
type IngestConfig struct {
	BatchMaxSize int
	BatchMaxWait time.Duration
	FlushTimeout time.Duration
}

func (c *IngestConfig) defaults() {
	if c.BatchMaxSize <= 0 {
		c.BatchMaxSize = 500
	}
	if c.BatchMaxWait <= 0 {
		c.BatchMaxWait = 150 * time.Millisecond
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = 10 * time.Second
	}
}

// item couples an event with its flush acknowledgment.
type item struct {
	ev   entity.AuditEvent
	done chan error
}

// Ingestor is the write-side pipeline.
type Ingestor struct {
	cfg   IngestConfig
	store Store
	dedup Deduper
	batch *batcher.Batcher[item]
	log   *zap.Logger

	accepted     atomic.Int64
	deduplicated atomic.Int64
	flushed      atomic.Int64
	flushErrors  atomic.Int64
}

// IngestStats is a point-in-time counters snapshot for metrics.
type IngestStats struct {
	Accepted     int64
	Deduplicated int64
	Flushed      int64
	FlushErrors  int64
}

// NewIngestor builds and starts the pipeline.
func NewIngestor(cfg IngestConfig, store Store, dedup Deduper, log *zap.Logger) *Ingestor {
	cfg.defaults()
	ing := &Ingestor{cfg: cfg, store: store, dedup: dedup, log: log}
	ing.batch = batcher.New[item](
		batcher.Config{MaxSize: cfg.BatchMaxSize, MaxWait: cfg.BatchMaxWait},
		ing.flush,
		nil, // per-item errors travel through item.done
	)
	return ing
}

// Close drains the pipeline (graceful shutdown).
func (i *Ingestor) Close() { i.batch.Close() }

// Backlog reports buffered-but-unflushed events (metrics/backpressure gauge).
func (i *Ingestor) Backlog() int { return i.batch.Len() }

// HandleAMQP ingests one raw AMQP payload. The returned error drives the
// broker retry/DLQ policy in pkg/rabbitmq.
func (i *Ingestor) HandleAMQP(ctx context.Context, body []byte) error {
	var msg messages.AuditMessage
	if err := jsonx.Unmarshal(body, &msg); err != nil {
		return err // undecodable → permanent → DLQ
	}
	return i.Ingest(ctx, msg.ToEntity())
}

// Ingest runs one event through dedup → batch → flush-ack.
func (i *Ingestor) Ingest(ctx context.Context, ev entity.AuditEvent) error {
	if err := validate(&ev); err != nil {
		return err
	}

	// Fast-path dedup: a hit means the event is already durably stored
	// (keys are only written after successful flush), so acking is safe.
	if seen, err := i.dedup.Seen(ctx, ev.ID.String()); err == nil && seen {
		i.deduplicated.Add(1)
		return nil
	} // On dedup-store error: fall through — PG ON CONFLICT is the backstop.

	it := item{ev: ev, done: make(chan error, 1)}
	i.batch.Add(it)
	i.accepted.Add(1)

	select {
	case err := <-it.done:
		return err
	case <-time.After(i.cfg.FlushTimeout):
		return apperrors.New(apperrors.CodeUnavailable, "audit flush timeout")
	case <-ctx.Done():
		return apperrors.Wrap(apperrors.CodeUnavailable, "ingest canceled", ctx.Err())
	}
}

// flush is the group commit: one idempotent batch insert, then per-item acks
// and dedup marking.
func (i *Ingestor) flush(ctx context.Context, batch []item) error {
	events := make([]entity.AuditEvent, len(batch))
	for n, it := range batch {
		events[n] = it.ev
	}

	_, err := i.store.InsertBatch(ctx, events)
	if err != nil {
		i.flushErrors.Add(1)
		wrapped := apperrors.Wrap(apperrors.CodeUnavailable, "audit batch insert", err)
		for _, it := range batch {
			it.done <- wrapped
		}
		return wrapped
	}

	i.flushed.Add(int64(len(batch)))
	for _, it := range batch {
		// Mark AFTER durable success — a pre-flush mark could drop a
		// redelivered event whose first flush failed.
		i.dedup.MarkSeen(ctx, it.ev.ID.String(), constants.AuditDedupTTL)
		it.done <- nil
	}
	return nil
}

// Stats snapshots the counters.
func (i *Ingestor) Stats() IngestStats {
	return IngestStats{
		Accepted:     i.accepted.Load(),
		Deduplicated: i.deduplicated.Load(),
		Flushed:      i.flushed.Load(),
		FlushErrors:  i.flushErrors.Load(),
	}
}

func validate(ev *entity.AuditEvent) error {
	if ev.ID == [16]byte{} {
		return apperrors.New(apperrors.CodeInvalidInput, "audit event id required")
	}
	if ev.Type == "" {
		return apperrors.New(apperrors.CodeInvalidInput, "audit event type required")
	}
	if ev.OccurredAt.IsZero() {
		ev.OccurredAt = time.Now().UTC()
	}
	if ev.Service == "" {
		ev.Service = "unknown"
	}
	return nil
}

// RunPartitionMaintainer pre-creates daily partitions ahead of time so
// midnight never blocks an insert.
func (i *Ingestor) RunPartitionMaintainer(ctx context.Context, ahead int, every time.Duration) error {
	if err := i.store.EnsurePartitions(ctx, ahead); err != nil {
		return err
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := i.store.EnsurePartitions(ctx, ahead); err != nil {
				i.log.Error("partition maintenance failed", zap.Error(err))
			}
		}
	}
}
