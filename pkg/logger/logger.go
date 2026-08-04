// Package logger builds the project-wide zap logger.
//
// Two personalities:
//   - dev:  a hand-crafted console encoder — colored, aligned, icon-tagged —
//     designed to make a multi-service compose log stream instantly scannable.
//   - prod: sampled JSON with RFC3339Nano timestamps for machine ingestion.
//
// Every log line automatically carries service name; WithTrace attaches
// trace_id/span_id so Grafana/Jaeger can cross-link log lines to spans.
package logger

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// Mode selects the logger personality.
type Mode string

const (
	ModeDev  Mode = "dev"
	ModeProd Mode = "prod"
)

// New constructs the root logger for a service.
func New(service string, mode Mode, level string) (*zap.Logger, error) {
	lvl, err := zapcore.ParseLevel(level)
	if err != nil {
		lvl = zapcore.InfoLevel
	}

	var core zapcore.Core
	switch mode {
	case ModeProd:
		encCfg := zap.NewProductionEncoderConfig()
		encCfg.TimeKey = "ts"
		encCfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder
		core = zapcore.NewCore(zapcore.NewJSONEncoder(encCfg), zapcore.Lock(os.Stdout), lvl)
		// Sampling: first 100 of each message per second pass, then 1 in 100 —
		// keeps hot-path logging near-zero-cost under load.
		core = zapcore.NewSamplerWithOptions(core, time.Second, 100, 100)
	default:
		core = zapcore.NewCore(newDevEncoder(), zapcore.Lock(os.Stdout), lvl)
	}

	log := zap.New(core,
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
		zap.Fields(zap.String("service", service)),
	)
	return log, nil
}

// WithTrace returns a child logger annotated with the span context found in
// ctx, if any. Log lines become clickable from trace views.
func WithTrace(ctx context.Context, log *zap.Logger) *zap.Logger {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return log
	}
	return log.With(
		zap.String("trace_id", sc.TraceID().String()),
		zap.String("span_id", sc.SpanID().String()),
	)
}

// ---- dev encoder ----------------------------------------------------------

const (
	ansiReset  = "\x1b[0m"
	ansiDim    = "\x1b[2m"
	ansiBold   = "\x1b[1m"
	ansiCyan   = "\x1b[36m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiRed    = "\x1b[31m"
	ansiMag    = "\x1b[35m"
	ansiBlue   = "\x1b[34m"
)

// levelBadge maps a level to its colored, fixed-width badge.
func levelBadge(l zapcore.Level) string {
	switch l {
	case zapcore.DebugLevel:
		return ansiDim + "✦ DEBUG" + ansiReset
	case zapcore.InfoLevel:
		return ansiGreen + "ℹ INFO " + ansiReset
	case zapcore.WarnLevel:
		return ansiYellow + "⚠ WARN " + ansiReset
	case zapcore.ErrorLevel:
		return ansiRed + "✖ ERROR" + ansiReset
	default:
		return ansiRed + ansiBold + "☠ " + l.CapitalString() + ansiReset
	}
}

// serviceColor gives each service a stable color so interleaved compose logs
// are visually separable at a glance.
func serviceColor(name string) string {
	switch name {
	case "orchestrator":
		return ansiCyan
	case "agent":
		return ansiMag
	case "audit":
		return ansiBlue
	default:
		return ansiGreen
	}
}

func newDevEncoder() zapcore.Encoder {
	cfg := zapcore.EncoderConfig{
		MessageKey:    "msg",
		LevelKey:      "level",
		TimeKey:       "ts",
		NameKey:       "logger",
		CallerKey:     "caller",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel: func(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(levelBadge(l))
		},
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(ansiDim + t.Format("15:04:05.000") + ansiReset)
		},
		EncodeCaller: func(c zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(ansiDim + c.TrimmedPath() + ansiReset)
		},
		EncodeDuration:   zapcore.StringDurationEncoder,
		ConsoleSeparator: "  ",
	}
	return &devEncoder{Encoder: zapcore.NewConsoleEncoder(cfg)}
}

// devEncoder wraps the console encoder to colorize the service field inline.
type devEncoder struct {
	zapcore.Encoder
}

func (d *devEncoder) Clone() zapcore.Encoder { return &devEncoder{Encoder: d.Encoder.Clone()} }

func (d *devEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	for i, f := range fields {
		if f.Key == "service" && f.Type == zapcore.StringType {
			color := serviceColor(f.String)
			fields[i] = zap.String("service", fmt.Sprintf("%s%s◈ %s%s", color, ansiBold, f.String, ansiReset))
		}
	}
	return d.Encoder.EncodeEntry(ent, fields)
}
