package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	httpv1 "github.com/KFN002/perfect-go-service/internal/controller/http/v1"
	"github.com/KFN002/perfect-go-service/pkg/rabbitmq"
)

// pprofPort is offset from nothing — it is a fixed private diagnostics port,
// never exposed by compose; reachable only via `docker exec` or port-forward.
const pprofPort = 6060

// runPprof serves net/http/pprof on localhost when enabled. Localhost binding
// inside the container plus no port mapping = defense in depth: profiling is
// an operator action, not an attack surface.
func runPprof(ctx context.Context, enabled bool, log *zap.Logger) func() error {
	return func() error {
		if !enabled {
			<-ctx.Done()
			return nil
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		srv := &http.Server{
			Addr:              fmt.Sprintf("localhost:%d", pprofPort),
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			<-ctx.Done()
			shCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = srv.Shutdown(shCtx)
		}()
		log.Info("pprof listening", zap.Int("port", pprofPort))
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

// stopGRPCGracefully drains in-flight RPCs but refuses to hang forever: a
// stuck stream cannot hold the process past the deadline (GracefulStop alone
// waits unboundedly — a classic shutdown footgun).
func stopGRPCGracefully(srv *grpc.Server, deadline time.Duration, log *zap.Logger) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(deadline):
		log.Warn("gRPC graceful drain exceeded deadline — forcing stop")
		srv.Stop()
		<-done
	}
}

// declareFlows declares the retry/DLQ topology for each flow, failing fast
// with a descriptive error.
func declareFlows(mq *rabbitmq.Client, flows ...rabbitmq.Flow) error {
	for _, flow := range flows {
		if err := mq.DeclareFlow(flow); err != nil {
			return fmt.Errorf("declare %s: %w", flow.Queue, err)
		}
	}
	return nil
}

// readiness assembles the standard dependency probes for /readyz.
func readiness(pg, rds func(context.Context) error, mq *rabbitmq.Client) []httpv1.ReadinessCheck {
	return []httpv1.ReadinessCheck{
		{Name: "postgres", Check: pg},
		{Name: "redis", Check: rds},
		{Name: "rabbitmq", Check: func(context.Context) error { return mq.Ping() }},
	}
}
