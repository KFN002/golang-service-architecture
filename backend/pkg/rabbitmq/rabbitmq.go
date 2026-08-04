// Package rabbitmq owns the AMQP topology and resilient publish/consume.
//
// Topology per logical flow F (declared idempotently at startup):
//
//	F.ex        direct exchange — producers publish here
//	F.q         quorum queue    — consumers read here; poison → F.dlq.ex
//	F.retry.ex  direct exchange — transient failures are republished here
//	F.retry.q   queue whose DLX routes back to F.ex; per-message TTL
//	            implements exponential backoff without any timer process
//	F.dlq.ex    direct exchange → F.dlq — the dead-letter parking lot
//
// Delivery guarantees: publisher confirms + persistent messages +
// manual acks + consumer idempotency = at-least-once, exactly-once effect.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/KFN002/perfect-go-service/pkg/apperrors"
	"github.com/KFN002/perfect-go-service/pkg/constants"
)

// Config for the broker connection.
type Config struct {
	URL           string
	ReconnectWait time.Duration
}

func (c *Config) defaults() {
	if c.ReconnectWait <= 0 {
		c.ReconnectWait = 2 * time.Second
	}
}

// Client manages one AMQP connection with automatic reconnect.
type Client struct {
	cfg Config
	log *zap.Logger

	mu   sync.RWMutex
	conn *amqp.Connection

	closed chan struct{}
	once   sync.Once
}

// New connects to the broker.
func New(cfg Config, log *zap.Logger) (*Client, error) {
	cfg.defaults()
	c := &Client{cfg: cfg, log: log, closed: make(chan struct{})}
	if err := c.connect(); err != nil {
		return nil, err
	}
	go c.reconnectLoop()
	return c, nil
}

// Close shuts the connection down.
func (c *Client) Close() {
	c.once.Do(func() { close(c.closed) })
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

// Channel opens a fresh channel on the live connection.
func (c *Client) Channel() (*amqp.Channel, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.conn == nil || c.conn.IsClosed() {
		return nil, apperrors.New(apperrors.CodeUnavailable, "amqp connection down")
	}
	return c.conn.Channel()
}

// Ping reports broker liveness (used by /readyz).
func (c *Client) Ping() error {
	ch, err := c.Channel()
	if err != nil {
		return err
	}
	return ch.Close()
}

// Flow names the exchange/queue family for one message stream.
type Flow struct {
	Exchange   string
	Queue      string
	RoutingKey string
}

func (f Flow) retryExchange() string { return f.Exchange + constants.RetrySuffix }
func (f Flow) retryQueue() string    { return f.Queue + constants.RetrySuffix }
func (f Flow) dlqExchange() string   { return f.Exchange + constants.DLQSuffix }
func (f Flow) dlq() string           { return f.Queue + constants.DLQSuffix }

// DeclareFlow declares the full retry/DLQ topology for a flow. Idempotent.
func (c *Client) DeclareFlow(f Flow) error {
	ch, err := c.Channel()
	if err != nil {
		return err
	}
	defer ch.Close() //nolint:errcheck

	// Main exchange and quorum queue; rejected messages dead-letter to DLQ.
	if err := ch.ExchangeDeclare(f.Exchange, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(f.Queue, true, false, false, false, amqp.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    f.dlqExchange(),
		"x-dead-letter-routing-key": f.RoutingKey,
	}); err != nil {
		return err
	}
	if err := ch.QueueBind(f.Queue, f.RoutingKey, f.Exchange, false, nil); err != nil {
		return err
	}

	// Retry loop: messages wait out their per-message TTL here, then their
	// death routes them back to the main exchange for another attempt.
	if err := ch.ExchangeDeclare(f.retryExchange(), "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(f.retryQueue(), true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    f.Exchange,
		"x-dead-letter-routing-key": f.RoutingKey,
	}); err != nil {
		return err
	}
	if err := ch.QueueBind(f.retryQueue(), f.RoutingKey, f.retryExchange(), false, nil); err != nil {
		return err
	}

	// Dead-letter parking lot.
	if err := ch.ExchangeDeclare(f.dlqExchange(), "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(f.dlq(), true, false, false, false, nil); err != nil {
		return err
	}
	return ch.QueueBind(f.dlq(), f.RoutingKey, f.dlqExchange(), false, nil)
}

// Message is one payload with propagation metadata.
type Message struct {
	Body        []byte
	Attempt     int
	Traceparent string
	EventID     string
}

func (m Message) headers() amqp.Table {
	// Attempts are single digits; guard the int32 narrowing anyway.
	attempt := min(m.Attempt, 1<<30)
	t := amqp.Table{constants.HeaderAttempt: int32(attempt)} //nolint:gosec // clamped above
	if m.Traceparent != "" {
		t[constants.HeaderTraceparent] = m.Traceparent
	}
	if m.EventID != "" {
		t[constants.HeaderEventID] = m.EventID
	}
	return t
}

// Publisher publishes with confirms on a dedicated channel.
type Publisher struct {
	c  *Client
	mu sync.Mutex
	ch *amqp.Channel
}

// NewPublisher opens a confirming channel.
func (c *Client) NewPublisher() (*Publisher, error) {
	p := &Publisher{c: c}
	if err := p.reopen(); err != nil {
		return nil, err
	}
	return p, nil
}

// Publish sends persistently and waits for the broker confirm — the message
// is on disk (quorum-replicated) before this returns nil.
func (p *Publisher) Publish(ctx context.Context, exchange, key string, msg Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch == nil || p.ch.IsClosed() {
		if err := p.reopen(); err != nil {
			return apperrors.Wrap(apperrors.CodeUnavailable, "publisher channel", err)
		}
	}
	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key,
		true, // mandatory: unroutable → returned, not silently dropped
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Headers:      msg.headers(),
			Body:         msg.Body,
		})
	if err != nil {
		return apperrors.Wrap(apperrors.CodeUnavailable, "publish", err)
	}
	ok, err := conf.WaitContext(ctx)
	if err != nil {
		return apperrors.Wrap(apperrors.CodeUnavailable, "confirm wait", err)
	}
	if !ok {
		return apperrors.New(apperrors.CodeUnavailable, "broker nacked publish")
	}
	return nil
}

// PublishRetry parks a copy on the retry queue with the given backoff as
// per-message TTL; expiry dead-letters it back to the main exchange.
func (p *Publisher) PublishRetry(ctx context.Context, f Flow, msg Message, backoff time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch == nil || p.ch.IsClosed() {
		if err := p.reopen(); err != nil {
			return apperrors.Wrap(apperrors.CodeUnavailable, "publisher channel", err)
		}
	}
	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, f.retryExchange(), f.RoutingKey,
		false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Expiration:   strconv.FormatInt(backoff.Milliseconds(), 10),
			Headers:      msg.headers(),
			Body:         msg.Body,
		})
	if err != nil {
		return apperrors.Wrap(apperrors.CodeUnavailable, "publish retry", err)
	}
	ok, err := conf.WaitContext(ctx)
	if err != nil || !ok {
		return apperrors.New(apperrors.CodeUnavailable, "retry publish not confirmed")
	}
	return nil
}

// PublishDLQ parks a poison/expired-attempts message on the DLQ.
func (p *Publisher) PublishDLQ(ctx context.Context, f Flow, msg Message) error {
	return p.Publish(ctx, f.dlqExchange(), f.RoutingKey, msg)
}

// Close closes the publisher channel.
func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch != nil {
		_ = p.ch.Close()
	}
}

// Delivery is one consumed message plus its metadata.
type Delivery struct {
	Body        []byte
	Attempt     int
	Traceparent string
	EventID     string
}

// Handler processes one delivery. Returned error classes drive routing:
// retryable → retry queue with backoff; permanent → DLQ; nil → ack.
type Handler func(ctx context.Context, d Delivery) error

// ConsumeConfig tunes one consumer loop.
type ConsumeConfig struct {
	Flow        Flow
	Prefetch    int
	MaxAttempts int
	Backoff     func(attempt int) time.Duration
	ConsumerTag string
	// Async, when set, dispatches each delivery through it (e.g. a worker
	// pool's blocking Submit). Prefetch bounds in-flight deliveries, the
	// pool bounds concurrency — backpressure composes end to end.
	Async func(fn func()) bool
}

// Consume runs the consumer loop until ctx is canceled. It re-establishes its
// channel on failure and implements the retry/DLQ policy around handler.
func (c *Client) Consume(ctx context.Context, cfg ConsumeConfig, pub *Publisher, handler Handler) error {
	for {
		if err := c.consumeOnce(ctx, cfg, pub, handler); err != nil {
			if ctx.Err() != nil {
				return nil //nolint:nilerr // cancellation is the clean-shutdown path
			}
			c.log.Warn("consumer channel lost, restarting",
				zap.String("queue", cfg.Flow.Queue), zap.Error(err))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(c.cfg.ReconnectWait):
			}
			continue
		}
		return nil
	}
}

func attemptFrom(h amqp.Table) int {
	switch v := h[constants.HeaderAttempt].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	default:
		return 0
	}
}

func stringHeader(h amqp.Table, key string) string {
	if s, ok := h[key].(string); ok {
		return s
	}
	return ""
}

// InspectDLQ peeks up to limit messages from a flow's DLQ without consuming
// them destructively (they are requeued). For the operator endpoint.
func (c *Client) InspectDLQ(f Flow, limit int) ([]Delivery, error) {
	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}
	defer ch.Close() //nolint:errcheck

	// Deliveries stay unacked during the peek — an immediate per-message
	// nack(requeue) would put the message back at the head and make this
	// loop read it again. One batched nack at the end requeues everything.
	out := make([]Delivery, 0, limit)
	var lastTag uint64
	for range limit {
		d, ok, err := ch.Get(f.dlq(), false)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		lastTag = d.DeliveryTag
		out = append(out, Delivery{
			Body:        d.Body,
			Attempt:     attemptFrom(d.Headers),
			Traceparent: stringHeader(d.Headers, constants.HeaderTraceparent),
			EventID:     stringHeader(d.Headers, constants.HeaderEventID),
		})
	}
	if lastTag > 0 {
		_ = ch.Nack(lastTag, true, true) // multiple=true: requeue the whole peek
	}
	return out, nil
}

// RequeueDLQ moves up to limit messages from the DLQ back onto the main
// exchange with a reset attempt counter (operator-initiated redrive).
func (c *Client) RequeueDLQ(ctx context.Context, f Flow, pub *Publisher, limit int) (int, error) {
	ch, err := c.Channel()
	if err != nil {
		return 0, err
	}
	defer ch.Close() //nolint:errcheck

	moved := 0
	for range limit {
		d, ok, err := ch.Get(f.dlq(), false)
		if err != nil {
			return moved, err
		}
		if !ok {
			break
		}
		msg := Message{
			Body:        d.Body,
			Attempt:     0,
			Traceparent: stringHeader(d.Headers, constants.HeaderTraceparent),
			EventID:     stringHeader(d.Headers, constants.HeaderEventID),
		}
		if err := pub.Publish(ctx, f.Exchange, f.RoutingKey, msg); err != nil {
			_ = d.Nack(false, true)
			return moved, err
		}
		_ = d.Ack(false)
		moved++
	}
	return moved, nil
}

// ---- unexported implementation ------------------------------------------

func (c *Client) connect() error {
	conn, err := amqp.Dial(c.cfg.URL)
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return nil
}

// reconnectLoop re-dials whenever the connection drops.
func (c *Client) reconnectLoop() {
	for {
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		errCh := make(chan *amqp.Error, 1)
		conn.NotifyClose(errCh)
		select {
		case <-c.closed:
			return
		case amqpErr := <-errCh:
			if amqpErr == nil {
				return // clean shutdown
			}
			c.log.Warn("amqp connection lost, reconnecting", zap.Error(amqpErr))
			for {
				select {
				case <-c.closed:
					return
				case <-time.After(c.cfg.ReconnectWait):
				}
				if err := c.connect(); err == nil {
					c.log.Info("amqp reconnected")
					break
				}
			}
		}
	}
}

func (p *Publisher) reopen() error {
	ch, err := p.c.Channel()
	if err != nil {
		return err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return err
	}
	p.ch = ch
	return nil
}

func (c *Client) consumeOnce(ctx context.Context, cfg ConsumeConfig, pub *Publisher, handler Handler) error {
	ch, err := c.Channel()
	if err != nil {
		return err
	}
	defer ch.Close() //nolint:errcheck

	if err := ch.Qos(cfg.Prefetch, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(cfg.Flow.Queue, cfg.ConsumerTag, false, false, false, false, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			if cfg.Async != nil {
				if !cfg.Async(func() { c.dispatch(ctx, cfg, pub, handler, d) }) {
					_ = d.Nack(false, true) // pool shutting down — requeue
				}
			} else {
				c.dispatch(ctx, cfg, pub, handler, d)
			}
		}
	}
}

// dispatch applies the fault-tolerance policy around one delivery.
func (c *Client) dispatch(ctx context.Context, cfg ConsumeConfig, pub *Publisher, handler Handler, d amqp.Delivery) {
	del := Delivery{
		Body:        d.Body,
		Attempt:     attemptFrom(d.Headers),
		Traceparent: stringHeader(d.Headers, constants.HeaderTraceparent),
		EventID:     stringHeader(d.Headers, constants.HeaderEventID),
	}

	err := handler(ctx, del)
	switch {
	case err == nil:
		_ = d.Ack(false)

	case apperrors.IsRetryable(err) && del.Attempt+1 < cfg.MaxAttempts:
		msg := Message{
			Body:        d.Body,
			Attempt:     del.Attempt + 1,
			Traceparent: del.Traceparent,
			EventID:     del.EventID,
		}
		if pubErr := pub.PublishRetry(ctx, cfg.Flow, msg, cfg.Backoff(del.Attempt)); pubErr != nil {
			// Could not park a retry copy — nack with requeue so the broker
			// redelivers; better duplicate handling than message loss.
			_ = d.Nack(false, true)
			return
		}
		_ = d.Ack(false)
		c.log.Warn("delivery scheduled for retry",
			zap.String("queue", cfg.Flow.Queue),
			zap.Int("attempt", del.Attempt+1), zap.Error(err))

	default:
		msg := Message{Body: d.Body, Attempt: del.Attempt, Traceparent: del.Traceparent, EventID: del.EventID}
		if pubErr := pub.PublishDLQ(ctx, cfg.Flow, msg); pubErr != nil {
			_ = d.Nack(false, true)
			return
		}
		_ = d.Ack(false)
		c.log.Error("delivery dead-lettered",
			zap.String("queue", cfg.Flow.Queue),
			zap.Int("attempt", del.Attempt), zap.Error(err))
	}
}
