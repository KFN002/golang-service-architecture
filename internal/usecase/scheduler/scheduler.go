// Package scheduler is the orchestrator's heart: it relays the transactional
// outbox to RabbitMQ (fan-out) and applies agent results (fan-in), unlocking
// dependent tasks until the expression DAG completes.
package scheduler

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
	"github.com/KFN002/perfect-go-service/pkg/constants"
	"github.com/KFN002/perfect-go-service/pkg/jsonx"
	"github.com/KFN002/perfect-go-service/pkg/otel"
)

// Config tunes the scheduler loops.
type Config struct {
	RelayInterval time.Duration // outbox poll cadence
	RelayBatch    int           // rows claimed per relay tick
	PruneInterval time.Duration // published-row cleanup cadence
}

func (c *Config) defaults() {
	if c.RelayInterval <= 0 {
		c.RelayInterval = 100 * time.Millisecond
	}
	if c.RelayBatch <= 0 {
		c.RelayBatch = 128
	}
	if c.PruneInterval <= 0 {
		c.PruneInterval = time.Minute
	}
}

// Scheduler runs the relay loop and the result fan-in.
type Scheduler struct {
	cfg      Config
	store    Store
	pub      Publisher
	notifier Notifier
	log      *zap.Logger
}

// New wires the scheduler.
func New(cfg Config, store Store, pub Publisher, notifier Notifier, log *zap.Logger) *Scheduler {
	cfg.defaults()
	return &Scheduler{cfg: cfg, store: store, pub: pub, notifier: notifier, log: log}
}

// RunOutboxRelay pumps the outbox until ctx ends. Multiple replicas run this
// concurrently; SKIP LOCKED partitions the work between them.
func (s *Scheduler) RunOutboxRelay(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.RelayInterval)
	defer ticker.Stop()
	prune := time.NewTicker(s.cfg.PruneInterval)
	defer prune.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-prune.C:
			if n, err := s.store.PruneOutbox(ctx); err == nil && n > 0 {
				s.log.Debug("outbox pruned", zap.Int64("rows", n))
			}
		case <-ticker.C:
			// Drain until the outbox is momentarily empty so bursts do not
			// wait a full tick per batch.
			for {
				n, err := s.relayOnce(ctx)
				if err != nil {
					s.log.Warn("outbox relay failed", zap.Error(err))
					break
				}
				if n < s.cfg.RelayBatch {
					break
				}
			}
		}
	}
}

func (s *Scheduler) relayOnce(ctx context.Context) (int, error) {
	return s.store.RelayOutbox(ctx, s.cfg.RelayBatch, func(entries []OutboxEntry) []int64 {
		published := make([]int64, 0, len(entries))
		for _, e := range entries {
			var err error
			switch e.Kind {
			case "task":
				err = s.pub.PublishTask(ctx, e.Payload, traceparentOf(e.Payload))
			case "audit":
				err = s.pub.PublishAudit(ctx, e.Payload, traceparentOf(e.Payload))
			default:
				s.log.Error("unknown outbox kind dropped", zap.String("kind", e.Kind))
				published = append(published, e.ID) // do not wedge the relay
				continue
			}
			if err != nil {
				// Leave unpublished; a later tick (or another replica) retries.
				s.log.Warn("outbox publish failed", zap.Int64("id", e.ID), zap.Error(err))
				continue
			}
			published = append(published, e.ID)
		}
		return published
	})
}

// traceparentOf recovers the traceparent embedded in an outbox payload so the
// broker hop joins the original trace.
func traceparentOf(payload []byte) string {
	var probe struct {
		Traceparent string `json:"traceparent"`
	}
	_ = jsonx.Unmarshal(payload, &probe)
	return probe.Traceparent
}

// HandleResult is the fan-in entrypoint: one agent message → state transition.
// Wire-level retries/DLQ are handled by pkg/rabbitmq around this.
func (s *Scheduler) HandleResult(ctx context.Context, body []byte, traceparent string) error {
	ctx = otel.ExtractTraceparent(ctx, traceparent)
	ctx, span := otel.Tracer("usecase.scheduler").Start(ctx, "HandleResult")
	defer span.End()

	var res messages.ResultMessage
	if err := jsonx.Unmarshal(body, &res); err != nil {
		return err // permanent → DLQ
	}

	switch res.Kind {
	case messages.ResultStarted:
		if err := s.store.ApplyStarted(ctx, res.TaskID, res.WorkerID, res.Attempt); err != nil {
			return err
		}
		s.notifier.Notify(ctx, messages.Event{
			Kind: "task.updated", ExpressionID: res.ExpressionID, TaskID: &res.TaskID,
			Status: string(entity.TaskRunning), WorkerID: res.WorkerID, At: time.Now().UTC(),
		})
		return nil

	case messages.ResultError:
		audit := []messages.AuditMessage{
			messages.NewAudit(entity.AuditTaskFailed, constants.ServiceOrchestrator,
				"task", res.TaskID.String(), otel.TraceIDFrom(ctx),
				map[string]any{"error": res.Error, "worker": res.WorkerID}),
			messages.NewAudit(entity.AuditExpressionFailed, constants.ServiceOrchestrator,
				"expression", res.ExpressionID.String(), otel.TraceIDFrom(ctx),
				map[string]any{"error": res.Error}),
		}
		events, err := s.store.ApplyFailure(ctx, res, audit)
		if err != nil {
			return err
		}
		s.emit(ctx, events)
		return nil

	default: // ResultOK
		audit := []messages.AuditMessage{
			messages.NewAudit(entity.AuditTaskDone, constants.ServiceOrchestrator,
				"task", res.TaskID.String(), otel.TraceIDFrom(ctx),
				map[string]any{"result": res.Result, "worker": res.WorkerID, "compute_ms": res.ComputeMs}),
		}
		events, err := s.store.ApplyResult(ctx, res, audit)
		if err != nil {
			return err
		}
		s.emit(ctx, events)
		return nil
	}
}

func (s *Scheduler) emit(ctx context.Context, events []messages.Event) {
	for _, ev := range events {
		s.notifier.Notify(ctx, ev)
	}
}
