package v1

import (
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"

	"github.com/KFN002/perfect-go-service/pkg/logger"
	"github.com/KFN002/perfect-go-service/pkg/otel"
	"github.com/KFN002/perfect-go-service/pkg/ratelimit"
)

// SecurityHeaders sets the standard hardening headers on every response.
func SecurityHeaders() fiber.Handler {
	return func(c fiber.Ctx) error {
		c.Set("X-Content-Type-Options", "nosniff")
		c.Set("X-Frame-Options", "DENY")
		c.Set("Referrer-Policy", "no-referrer")
		c.Set("Cross-Origin-Opener-Policy", "same-origin")
		c.Set("Cross-Origin-Resource-Policy", "same-origin")
		c.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		c.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		return c.Next()
	}
}

// RateLimit throttles per client IP — the application layer of defense in
// depth (nginx limit_req sits in front, the gRPC interceptor behind).
func RateLimit(limiter *ratelimit.Limiter) fiber.Handler {
	return func(c fiber.Ctx) error {
		if !limiter.Allow(clientIP(c)) {
			c.Set("Retry-After", "1")
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"code":    "RATE_LIMITED",
				"message": "rate limit exceeded, retry later",
			})
		}
		return c.Next()
	}
}

// clientIP prefers the first X-Forwarded-For hop (set by nginx) and falls
// back to the socket address.
func clientIP(c fiber.Ctx) string {
	if xff := c.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i > 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	return c.IP()
}

// Trace opens a server span per request, honoring incoming W3C context —
// browser spans and nginx forwards join the same trace.
func Trace(service string) fiber.Handler {
	return func(c fiber.Ctx) error {
		ctx := otel.ExtractTraceparent(c.Context(), c.Get("traceparent"))
		ctx, span := otel.Tracer(service).Start(ctx, c.Method()+" "+c.Path())
		defer span.End()
		c.SetContext(ctx)
		return c.Next()
	}
}

// AccessLog writes one structured line per request with trace correlation.
func AccessLog(log *zap.Logger) fiber.Handler {
	return func(c fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		logger.WithTrace(c.Context(), log).Info("http",
			zap.String("method", c.Method()),
			zap.String("path", c.Path()),
			zap.Int("status", c.Response().StatusCode()),
			zap.Duration("took", time.Since(start)),
			zap.String("ip", clientIP(c)),
		)
		return err
	}
}
