package bootstrap

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type databasePinger interface {
	Ping(context.Context) error
}

type readinessSource interface {
	Ready() bool
}

type readinessMonitor struct {
	database  databasePinger
	consumers []namedReadinessSource
	health    *health.Server
	logger    *slog.Logger
	interval  time.Duration
	timeout   time.Duration
}

type namedReadinessSource struct {
	name   string
	source readinessSource
}

func (m *readinessMonitor) Run(ctx context.Context) error {
	defer m.setServing(false, "shutdown")
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	lastReady := false
	for {
		ready, reason := m.check(ctx)
		if ready != lastReady {
			m.setServing(ready, reason)
			lastReady = ready
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (m *readinessMonitor) check(ctx context.Context) (bool, string) {
	for _, consumer := range m.consumers {
		if !consumer.source.Ready() {
			return false, consumer.name
		}
	}
	checkCtx, cancel := context.WithTimeout(ctx, m.timeout)
	err := m.database.Ping(checkCtx)
	cancel()
	if err != nil {
		return false, "postgresql"
	}
	return true, "dependencies_ready"
}

func (m *readinessMonitor) setServing(ready bool, reason string) {
	status := healthpb.HealthCheckResponse_NOT_SERVING
	if ready {
		status = healthpb.HealthCheckResponse_SERVING
	}
	m.health.SetServingStatus(OverallHealthService, status)
	m.health.SetServingStatus(WorkerHealthService, status)
	m.logger.Info("readiness changed", "ready", ready, "reason", reason)
}
