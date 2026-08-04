// Package v1 contains the AMQP consumers of all three services. Each consumer
// adapts one queue to one usecase; retry/DLQ policy lives in pkg/rabbitmq.
package v1

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/internal/usecase/audit"
	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
	"github.com/KFN002/perfect-go-service/internal/usecase/scheduler"
	"github.com/KFN002/perfect-go-service/internal/usecase/worker"
	"github.com/KFN002/perfect-go-service/pkg/apperrors"
	"github.com/KFN002/perfect-go-service/pkg/bulkhead"
	"github.com/KFN002/perfect-go-service/pkg/constants"
	"github.com/KFN002/perfect-go-service/pkg/jsonx"
	"github.com/KFN002/perfect-go-service/pkg/otel"
	"github.com/KFN002/perfect-go-service/pkg/rabbitmq"
	"github.com/KFN002/perfect-go-service/pkg/retry"
	"github.com/KFN002/perfect-go-service/pkg/workerpool"
)

// Flows shared by producers and consumers.
var (
	TasksFlow = rabbitmq.Flow{
		Exchange: constants.TasksExchange, Queue: constants.TasksQueue,
		RoutingKey: constants.TasksRoutingKey,
	}
	ResultsFlow = rabbitmq.Flow{
		Exchange: constants.ResultsExchange, Queue: constants.ResultsQueue,
		RoutingKey: constants.ResultsRoutingKey,
	}
	AuditFlow = rabbitmq.Flow{
		Exchange: constants.AuditExchange, Queue: constants.AuditQueue,
		RoutingKey: constants.AuditRoutingKey,
	}
)

func backoff(attempt int) time.Duration {
	return retry.Backoff(retry.Config{Base: 500 * time.Millisecond, Cap: 30 * time.Second}, attempt)
}

// ---- orchestrator: results fan-in -----------------------------------------

// RunResultsConsumer feeds agent results into the scheduler.
func RunResultsConsumer(ctx context.Context, client *rabbitmq.Client, pub *rabbitmq.Publisher,
	sched *scheduler.Scheduler, prefetch int) error {
	return client.Consume(ctx, rabbitmq.ConsumeConfig{
		Flow:        ResultsFlow,
		Prefetch:    prefetch,
		MaxAttempts: 5,
		Backoff:     backoff,
		ConsumerTag: "orchestrator-results",
	}, pub, func(ctx context.Context, d rabbitmq.Delivery) error {
		return sched.HandleResult(ctx, d.Body, d.Traceparent)
	})
}

// ---- agent: task compute --------------------------------------------------

// AgentConsumer wires the tasks queue into the auto-scaling worker pool.
type AgentConsumer struct {
	computer *worker.Computer
	pool     *workerpool.Pool
	pub      *rabbitmq.Publisher
	log      *zap.Logger
}

// NewAgentConsumer builds the agent-side consumer.
func NewAgentConsumer(computer *worker.Computer, pool *workerpool.Pool,
	pub *rabbitmq.Publisher, log *zap.Logger) *AgentConsumer {
	return &AgentConsumer{computer: computer, pool: pool, pub: pub, log: log}
}

// Run consumes tasks until ctx ends. Prefetch ≈ pool max so the queue keeps
// the pool fed without hoarding messages other agent replicas could take.
func (a *AgentConsumer) Run(ctx context.Context, client *rabbitmq.Client, prefetch int) error {
	return client.Consume(ctx, rabbitmq.ConsumeConfig{
		Flow:        TasksFlow,
		Prefetch:    prefetch,
		MaxAttempts: 5,
		Backoff:     backoff,
		ConsumerTag: "agent-" + a.computer.WorkerID(),
		// Fan-out into the auto-scaling pool; Submit blocks when the pool
		// queue is full — backpressure all the way to the broker prefetch.
		Async: func(fn func()) bool {
			return a.pool.Submit(func(context.Context) { fn() })
		},
	}, a.pub, a.handle)
}

func (a *AgentConsumer) handle(ctx context.Context, d rabbitmq.Delivery) error {
	ctx = otel.ExtractTraceparent(ctx, d.Traceparent)
	ctx, span := otel.Tracer("agent").Start(ctx, "ComputeTask")
	defer span.End()

	var task messages.TaskMessage
	if err := jsonx.Unmarshal(d.Body, &task); err != nil {
		return err // poison → DLQ
	}
	task.Attempt = d.Attempt

	// Lifecycle: tell the orchestrator the task is running. Best effort by
	// design — a lost "started" ping costs a UI transition, nothing else.
	_ = a.publishResult(ctx, messages.ResultMessage{
		Kind: messages.ResultStarted, TaskID: task.TaskID,
		ExpressionID: task.ExpressionID, WorkerID: a.computer.WorkerID(),
		Attempt: task.Attempt,
	}, d.Traceparent)

	start := time.Now()
	value, err := a.computer.Compute(ctx, task)
	elapsed := time.Since(start)

	switch {
	case err == nil:
		return a.publishResult(ctx, messages.ResultMessage{
			Kind: messages.ResultOK, TaskID: task.TaskID,
			ExpressionID: task.ExpressionID, Result: value,
			WorkerID: a.computer.WorkerID(), Attempt: task.Attempt,
			ComputeMs: elapsed.Milliseconds(),
		}, d.Traceparent)

	case apperrors.IsRetryable(err):
		return err // transient (shutdown mid-compute) → broker retry loop

	default:
		// Permanent computation failure — report it as a result, ack the task.
		return a.publishResult(ctx, messages.ResultMessage{
			Kind: messages.ResultError, TaskID: task.TaskID,
			ExpressionID: task.ExpressionID, Error: err.Error(),
			ErrorCode: string(apperrors.CodeOf(err)),
			WorkerID:  a.computer.WorkerID(), Attempt: task.Attempt,
		}, d.Traceparent)
	}
}

func (a *AgentConsumer) publishResult(ctx context.Context, res messages.ResultMessage, traceparent string) error {
	body, err := jsonx.Marshal(res)
	if err != nil {
		return err
	}
	return a.pub.Publish(ctx, ResultsFlow.Exchange, ResultsFlow.RoutingKey, rabbitmq.Message{
		Body: body, Attempt: res.Attempt, Traceparent: traceparent,
		EventID: res.TaskID.String() + ":" + string(res.Kind),
	})
}

// PublishPoolScaled emits an audit event when the agent pool resizes —
// autoscaling becomes observable on the dashboard.
func PublishPoolScaled(pub *rabbitmq.Publisher, workerID string, log *zap.Logger) func(from, to int32, reason string) {
	return func(from, to int32, reason string) {
		msg := messages.NewAudit(entity.AuditPoolScaled, constants.ServiceAgent,
			"pool", workerID, "", map[string]any{
				"from": from, "to": to, "reason": reason,
			})
		body, err := jsonx.Marshal(msg)
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := pub.Publish(ctx, AuditFlow.Exchange, AuditFlow.RoutingKey, rabbitmq.Message{
			Body: body, EventID: msg.ID.String(),
		}); err != nil {
			log.Debug("pool-scaled audit publish failed", zap.Error(err))
		}
	}
}

// ---- audit: bulkheaded ingestion ------------------------------------------

// RunAuditConsumer feeds the audit queue into the ingest pipeline. The
// bulkhead isolates AMQP ingestion capacity from the gRPC write path.
func RunAuditConsumer(ctx context.Context, client *rabbitmq.Client, pub *rabbitmq.Publisher,
	ingest *audit.Ingestor, bh *bulkhead.Bulkhead, prefetch int) error {
	return client.Consume(ctx, rabbitmq.ConsumeConfig{
		Flow:        AuditFlow,
		Prefetch:    prefetch,
		MaxAttempts: 8,
		Backoff:     backoff,
		ConsumerTag: "audit-ingester",
		Async:       goDispatch, // concurrent handling; prefetch bounds it
	}, pub, func(ctx context.Context, d rabbitmq.Delivery) error {
		return bh.DoWait(ctx, func(ctx context.Context) error {
			return ingest.HandleAMQP(ctx, d.Body)
		})
	})
}

// goDispatch runs deliveries on plain goroutines; the AMQP prefetch window is
// the concurrency bound (each unacked delivery is one in-flight goroutine).
func goDispatch(fn func()) bool {
	go fn()
	return true
}
