// Package config loads service configuration from environment variables with
// sane defaults. Env-first keeps containers 12-factor; .env.example documents
// every knob.
package config

import (
	"os"
	"strconv"
	"time"

	"github.com/KFN002/perfect-go-service/internal/usecase/worker"
	"github.com/KFN002/perfect-go-service/pkg/logger"
)

// Common holds settings shared by every service.
type Common struct {
	Env          logger.Mode // dev | prod
	LogLevel     string
	InstanceID   string // unique per replica (defaults to hostname)
	OTelEnabled  bool
	OTelEndpoint string
	PprofEnabled bool
}

// Orchestrator is the orchestrator service configuration.
type Orchestrator struct {
	Common
	HTTPPort      int
	GRPCPort      int
	PGDSN         string
	RedisAddr     string
	RabbitURL     string
	RelayInterval time.Duration
	RelayBatch    int
	Prefetch      int
	RateRPS       float64
	RateBurst     float64
}

// Agent is the agent service configuration.
type Agent struct {
	Common
	HTTPPort  int
	RabbitURL string
	PoolMin   int
	PoolMax   int
	PoolIdle  time.Duration
	Prefetch  int
	Latencies worker.Latencies
}

// Audit is the audit service configuration.
type Audit struct {
	Common
	HTTPPort        int
	GRPCPort        int
	PGDSN           string
	RedisAddr       string
	RabbitURL       string
	BatchMaxSize    int
	BatchMaxWait    time.Duration
	IngestBulk      int // AMQP ingestion bulkhead capacity
	WriteBulk       int // gRPC write bulkhead capacity
	Prefetch        int
	RateRPS         float64
	RateBurst       float64
	QueryTTL        time.Duration
	PartitionsAhead int
}

// LoadOrchestrator reads the orchestrator config from the environment.
func LoadOrchestrator() Orchestrator {
	return Orchestrator{
		Common:        common("orchestrator"),
		HTTPPort:      envInt("HTTP_PORT", 8080),
		GRPCPort:      envInt("GRPC_PORT", 50051),
		PGDSN:         envStr("PG_DSN", "postgres://calc:calc@localhost:5432/calc?sslmode=disable"),
		RedisAddr:     envStr("REDIS_ADDR", "localhost:6379"),
		RabbitURL:     envStr("RABBIT_URL", "amqp://calc:calc@localhost:5672/"),
		RelayInterval: envDur("RELAY_INTERVAL", 100*time.Millisecond),
		RelayBatch:    envInt("RELAY_BATCH", 128),
		Prefetch:      envInt("PREFETCH", 64),
		RateRPS:       envFloat("RATE_RPS", 50),
		RateBurst:     envFloat("RATE_BURST", 100),
	}
}

// LoadAgent reads the agent config from the environment.
func LoadAgent() Agent {
	return Agent{
		Common:    common("agent"),
		HTTPPort:  envInt("HTTP_PORT", 8082),
		RabbitURL: envStr("RABBIT_URL", "amqp://calc:calc@localhost:5672/"),
		PoolMin:   envInt("POOL_MIN", 2),
		PoolMax:   envInt("POOL_MAX", 32),
		PoolIdle:  envDur("POOL_IDLE_TIMEOUT", 15*time.Second),
		Prefetch:  envInt("PREFETCH", 64),
		Latencies: worker.Latencies{
			Add: envDur("OP_LATENCY_ADD", time.Second),
			Sub: envDur("OP_LATENCY_SUB", time.Second),
			Mul: envDur("OP_LATENCY_MUL", 2*time.Second),
			Div: envDur("OP_LATENCY_DIV", 2*time.Second),
		},
	}
}

// LoadAudit reads the audit config from the environment.
func LoadAudit() Audit {
	return Audit{
		Common:          common("audit"),
		HTTPPort:        envInt("HTTP_PORT", 8081),
		GRPCPort:        envInt("GRPC_PORT", 50052),
		PGDSN:           envStr("PG_DSN", "postgres://audit:audit@localhost:5433/audit?sslmode=disable"),
		RedisAddr:       envStr("REDIS_ADDR", "localhost:6380"),
		RabbitURL:       envStr("RABBIT_URL", "amqp://calc:calc@localhost:5672/"),
		BatchMaxSize:    envInt("BATCH_MAX_SIZE", 500),
		BatchMaxWait:    envDur("BATCH_MAX_WAIT", 150*time.Millisecond),
		IngestBulk:      envInt("INGEST_BULKHEAD", 64),
		WriteBulk:       envInt("WRITE_BULKHEAD", 16),
		Prefetch:        envInt("PREFETCH", 256),
		RateRPS:         envFloat("RATE_RPS", 100),
		RateBurst:       envFloat("RATE_BURST", 200),
		QueryTTL:        envDur("QUERY_CACHE_TTL", 3*time.Second),
		PartitionsAhead: envInt("PARTITIONS_AHEAD", 3),
	}
}

func common(service string) Common {
	host, _ := os.Hostname()
	if host == "" {
		host = service
	}
	mode := logger.ModeDev
	if envStr("APP_ENV", "dev") == "prod" {
		mode = logger.ModeProd
	}
	return Common{
		Env:          mode,
		LogLevel:     envStr("LOG_LEVEL", "info"),
		InstanceID:   envStr("INSTANCE_ID", host),
		OTelEnabled:  envBool("OTEL_ENABLED", true),
		OTelEndpoint: envStr("OTEL_ENDPOINT", "localhost:4317"),
		PprofEnabled: envBool("PPROF_ENABLED", false),
	}
}

// ---- env helpers -----------------------------------------------------------

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
