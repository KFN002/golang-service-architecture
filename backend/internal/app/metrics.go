package app

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	httpv1 "github.com/KFN002/perfect-go-service/internal/controller/http/v1"
	"github.com/KFN002/perfect-go-service/internal/usecase/audit"
	"github.com/KFN002/perfect-go-service/pkg/bulkhead"
	"github.com/KFN002/perfect-go-service/pkg/workerpool"
)

// registerOrchestratorMetrics exposes orchestrator-specific gauges.
func registerOrchestratorMetrics(hub *httpv1.SSEHub) {
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "calc", Subsystem: "sse", Name: "clients",
		Help: "Connected SSE dashboard clients.",
	}, func() float64 { return float64(hub.Clients()) })
}

// registerAgentMetrics exposes live worker-pool gauges — the autoscaling
// curve on the Grafana/workers dashboards comes from here.
func registerAgentMetrics(instance string, pool *workerpool.Pool) {
	labels := prometheus.Labels{"instance_id": instance}
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "calc", Subsystem: "pool", Name: "workers", ConstLabels: labels,
		Help: "Current goroutine workers in the pool.",
	}, func() float64 { return float64(pool.Stats().Workers) })
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "calc", Subsystem: "pool", Name: "busy", ConstLabels: labels,
		Help: "Workers currently computing.",
	}, func() float64 { return float64(pool.Stats().Busy) })
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "calc", Subsystem: "pool", Name: "backlog", ConstLabels: labels,
		Help: "Tasks queued in the pool.",
	}, func() float64 { return float64(pool.Stats().Backlog) })
	promauto.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "calc", Subsystem: "pool", Name: "done_total", ConstLabels: labels,
		Help: "Tasks completed by the pool.",
	}, func() float64 { return float64(pool.Stats().Done) })
}

// registerAuditMetrics exposes the write-pipeline gauges.
func registerAuditMetrics(ing *audit.Ingestor, bh *bulkhead.Bulkhead) {
	promauto.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "audit", Name: "accepted_total", Help: "Events accepted into the pipeline.",
	}, func() float64 { return float64(ing.Stats().Accepted) })
	promauto.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "audit", Name: "deduplicated_total", Help: "Duplicate events dropped.",
	}, func() float64 { return float64(ing.Stats().Deduplicated) })
	promauto.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "audit", Name: "flushed_total", Help: "Events durably flushed.",
	}, func() float64 { return float64(ing.Stats().Flushed) })
	promauto.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "audit", Name: "flush_errors_total", Help: "Failed batch flushes.",
	}, func() float64 { return float64(ing.Stats().FlushErrors) })
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "audit", Name: "backlog", Help: "Events buffered awaiting flush.",
	}, func() float64 { return float64(ing.Backlog()) })
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "audit", Name: "ingest_in_flight", Help: "AMQP ingest bulkhead occupancy.",
	}, func() float64 { inFlight, _ := bh.Stats(); return float64(inFlight) })
	promauto.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "audit", Name: "ingest_rejected_total", Help: "Bulkhead rejections (load shed).",
	}, func() float64 { _, rejected := bh.Stats(); return float64(rejected) })
}
