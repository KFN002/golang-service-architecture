package v1

import (
	"bufio"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"

	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
	"github.com/KFN002/perfect-go-service/pkg/jsonx"
)

// SSEHub fans Redis pub/sub events out to connected dashboard clients.
//
// Each client owns a buffered channel; a slow client's buffer overflow drops
// events for that client only (drop-slowest policy) — one stuck browser can
// never back-pressure the hub or other clients.
type SSEHub struct {
	mu      sync.RWMutex
	clients map[int64]chan []byte
	nextID  atomic.Int64
	count   atomic.Int64
	log     *zap.Logger
}

// NewSSEHub builds the hub.
func NewSSEHub(log *zap.Logger) *SSEHub {
	return &SSEHub{clients: make(map[int64]chan []byte), log: log}
}

// Broadcast delivers one raw event payload to every client.
func (h *SSEHub) Broadcast(payload string) {
	data := []byte(payload)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ch := range h.clients {
		select {
		case ch <- data:
		default: // slow client — drop for them, never block the hub
		}
	}
}

// Clients reports the live connection count (metrics).
func (h *SSEHub) Clients() int64 { return h.count.Load() }

func (h *SSEHub) subscribe() (int64, chan []byte) {
	id := h.nextID.Add(1)
	ch := make(chan []byte, 64)
	h.mu.Lock()
	h.clients[id] = ch
	h.mu.Unlock()
	h.count.Add(1)
	return id, ch
}

func (h *SSEHub) unsubscribe(id int64) {
	h.mu.Lock()
	delete(h.clients, id)
	h.mu.Unlock()
	h.count.Add(-1)
}

// Handler serves GET /api/v1/events. Optional ?expression_id= filters the
// stream server-side so the browser only parses what it renders.
func (h *SSEHub) Handler(c fiber.Ctx) error {
	filter := c.Query("expression_id")

	c.Set(fiber.HeaderContentType, "text/event-stream")
	c.Set(fiber.HeaderCacheControl, "no-cache")
	c.Set(fiber.HeaderConnection, "keep-alive")
	c.Set("X-Accel-Buffering", "no") // nginx: do not buffer this response

	id, ch := h.subscribe()
	ctx := c.RequestCtx()

	return c.SendStreamWriter(func(w *bufio.Writer) {
		defer h.unsubscribe(id)
		// Initial comment establishes the stream through proxies.
		_, _ = fmt.Fprint(w, ": connected\n\n")
		_ = w.Flush()

		for {
			select {
			case <-ctx.Done():
				return
			case data := <-ch:
				if filter != "" && !eventMatches(data, filter) {
					continue
				}
				_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
				if err := w.Flush(); err != nil {
					return // client went away
				}
			}
		}
	})
}

func eventMatches(data []byte, exprID string) bool {
	var ev messages.Event
	if err := jsonx.Unmarshal(data, &ev); err != nil {
		return false
	}
	return ev.ExpressionID.String() == exprID
}

// RunRedisBridge pumps the Redis events channel into the hub until ctx ends.
func (h *SSEHub) RunRedisBridge(ctx context.Context, subscribe func(ctx context.Context, channel string, fn func(payload string)) error, channel string) error {
	err := subscribe(ctx, channel, h.Broadcast)
	if ctx.Err() != nil {
		return nil //nolint:nilerr // context cancellation IS the clean shutdown path
	}
	return err
}
