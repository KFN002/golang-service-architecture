package v1

import (
	"context"

	"github.com/KFN002/perfect-go-service/pkg/circuitbreaker"
	"github.com/KFN002/perfect-go-service/pkg/rabbitmq"
	"github.com/KFN002/perfect-go-service/pkg/retry"
)

// GuardedPublisher implements scheduler.Publisher: every publish goes through
// a circuit breaker (fail fast when the broker is down) and jittered retries
// (ride out blips). The outbox above it guarantees nothing is ever lost.
type GuardedPublisher struct {
	pub     *rabbitmq.Publisher
	breaker *circuitbreaker.Breaker
	retry   retry.Config
}

// NewGuardedPublisher wraps a confirming publisher with fault tolerance.
func NewGuardedPublisher(pub *rabbitmq.Publisher, breaker *circuitbreaker.Breaker) *GuardedPublisher {
	return &GuardedPublisher{
		pub:     pub,
		breaker: breaker,
		retry:   retry.Config{Attempts: 3},
	}
}

// PublishTask sends one task message to the tasks flow.
func (g *GuardedPublisher) PublishTask(ctx context.Context, body []byte, traceparent string) error {
	return g.publish(ctx, TasksFlow, body, traceparent)
}

// PublishAudit sends one audit message to the audit flow.
func (g *GuardedPublisher) PublishAudit(ctx context.Context, body []byte, traceparent string) error {
	return g.publish(ctx, AuditFlow, body, traceparent)
}

func (g *GuardedPublisher) publish(ctx context.Context, flow rabbitmq.Flow, body []byte, traceparent string) error {
	return retry.Do(ctx, g.retry, func(ctx context.Context) error {
		return g.breaker.Do(ctx, func(ctx context.Context) error {
			return g.pub.Publish(ctx, flow.Exchange, flow.RoutingKey, rabbitmq.Message{
				Body: body, Traceparent: traceparent,
			})
		})
	})
}
