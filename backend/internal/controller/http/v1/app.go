// Package v1 builds the Fiber v3 HTTP applications of the services: the
// grpc-gateway mount, SSE, health, metrics and operator endpoints — all
// behind the security middleware stack.
package v1

import (
	"context"
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	auditpb "github.com/KFN002/perfect-go-service/gen/audit/v1"
	calcpb "github.com/KFN002/perfect-go-service/gen/calc/v1"
	amqpv1 "github.com/KFN002/perfect-go-service/internal/controller/amqp/v1"
	auditgrpc "github.com/KFN002/perfect-go-service/internal/controller/grpc/auditv1"
	grpcv1 "github.com/KFN002/perfect-go-service/internal/controller/grpc/v1"
	"github.com/KFN002/perfect-go-service/pkg/constants"
	"github.com/KFN002/perfect-go-service/pkg/jsonx"
	"github.com/KFN002/perfect-go-service/pkg/rabbitmq"
	"github.com/KFN002/perfect-go-service/pkg/ratelimit"
)

const (
	jsonKeyStatus = "status"
	jsonKeyCode   = "code"
)

// ReadinessCheck probes one dependency.
type ReadinessCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

// AppConfig assembles one Fiber application.
type AppConfig struct {
	ServiceName string
	Log         *zap.Logger
	Limiter     *ratelimit.Limiter
	Readiness   []ReadinessCheck
}

// newApp builds the base Fiber app with the shared hardening stack.
func newApp(cfg AppConfig) *fiber.App {
	app := fiber.New(fiber.Config{
		AppName:      cfg.ServiceName,
		ReadTimeout:  constants.DefaultHTTPReadTimeout,
		WriteTimeout: constants.DefaultHTTPWriteTimeout,
		IdleTimeout:  constants.DefaultHTTPIdleTimeout,
		BodyLimit:    constants.MaxBodyBytes,
		JSONEncoder:  jsonx.Marshal,
		JSONDecoder:  jsonx.Unmarshal,
	})

	app.Use(recover.New())
	app.Use(SecurityHeaders())
	app.Use(Trace(cfg.ServiceName))
	app.Use(AccessLog(cfg.Log))

	// Health endpoints stay un-throttled (probes must never be shed);
	// everything else passes the limiter.
	registerHealth(app, cfg.Readiness)
	app.Use(RateLimit(cfg.Limiter))
	return app
}

func registerHealth(app *fiber.App, checks []ReadinessCheck) {
	app.Get("/healthz", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{jsonKeyStatus: "ok"})
	})
	app.Get("/readyz", func(c fiber.Ctx) error {
		for _, chk := range checks {
			if err := chk.Check(c.Context()); err != nil {
				return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
					jsonKeyStatus: "degraded", "failing": chk.Name,
				})
			}
		}
		return c.JSON(fiber.Map{jsonKeyStatus: "ready"})
	})
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))
}

// NewOrchestratorApp mounts the expression gateway, SSE and DLQ operations.
func NewOrchestratorApp(cfg AppConfig, exprSrv *grpcv1.Server, hub *SSEHub,
	mq *rabbitmq.Client, pub *rabbitmq.Publisher) (*fiber.App, error) {
	app := newApp(cfg)

	// In-process gateway: REST → generated stubs → gRPC server implementation
	// directly (no loopback dial, no extra hop).
	mux := runtime.NewServeMux(runtime.WithIncomingHeaderMatcher(traceHeaderMatcher))
	if err := calcpb.RegisterExpressionServiceHandlerServer(context.Background(), mux, exprSrv); err != nil {
		return nil, err
	}

	app.Get("/api/v1/events", hub.Handler)
	registerDLQ(app, mq, pub)
	app.All("/api/v1/*", adaptor.HTTPHandler(mux))
	return app, nil
}

// NewAuditApp mounts the audit gateway.
func NewAuditApp(cfg AppConfig, auditSrv *auditgrpc.Server) (*fiber.App, error) {
	app := newApp(cfg)

	mux := runtime.NewServeMux(runtime.WithIncomingHeaderMatcher(traceHeaderMatcher))
	if err := auditpb.RegisterAuditServiceHandlerServer(context.Background(), mux, auditSrv); err != nil {
		return nil, err
	}
	app.All("/api/v1/audit/*", adaptor.HTTPHandler(mux))
	return app, nil
}

// registerDLQ exposes the operator endpoints for dead-letter inspection and
// redrive — failure handling made visible and reversible.
func registerDLQ(app *fiber.App, mq *rabbitmq.Client, pub *rabbitmq.Publisher) {
	flows := map[string]rabbitmq.Flow{
		"tasks":   amqpv1.TasksFlow,
		"results": amqpv1.ResultsFlow,
	}

	app.Get("/api/v1/dlq/:flow", func(c fiber.Ctx) error {
		flow, ok := flows[c.Params("flow")]
		if !ok {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{jsonKeyCode: "NOT_FOUND"})
		}
		items, err := mq.InspectDLQ(flow, 50)
		if err != nil {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{jsonKeyCode: "UNAVAILABLE"})
		}
		out := make([]fiber.Map, 0, len(items))
		for _, it := range items {
			out = append(out, fiber.Map{
				"body":        string(it.Body),
				"attempt":     it.Attempt,
				"traceparent": it.Traceparent,
			})
		}
		return c.JSON(fiber.Map{"flow": c.Params("flow"), "messages": out})
	})

	app.Post("/api/v1/dlq/:flow/requeue", func(c fiber.Ctx) error {
		flow, ok := flows[c.Params("flow")]
		if !ok {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{jsonKeyCode: "NOT_FOUND"})
		}
		moved, err := mq.RequeueDLQ(c.Context(), flow, pub, 100)
		if err != nil {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{jsonKeyCode: "UNAVAILABLE"})
		}
		return c.JSON(fiber.Map{"requeued": moved})
	})
}

// traceHeaderMatcher forwards the W3C trace header into gRPC metadata so the
// in-process servers can join the caller's trace; everything else follows the
// gateway's default policy.
func traceHeaderMatcher(key string) (string, bool) {
	if strings.EqualFold(key, "traceparent") {
		return "traceparent", true
	}
	return runtime.DefaultHeaderMatcher(key)
}

// Keep net/http imported for the promhttp adaptor path.
var _ = http.StatusOK
