package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestReadinessRequiresConsumerAndDatabase(t *testing.T) {
	t.Parallel()
	healthServer := health.NewServer()
	dispatchConsumer := &fakeReadinessSource{}
	lifecycleConsumer := &fakeReadinessSource{}
	database := &fakeDatabasePinger{}
	monitor := &readinessMonitor{
		database: database,
		consumers: []namedReadinessSource{
			{name: "rabbitmq_dispatch_consumer", source: dispatchConsumer},
			{name: "rabbitmq_lifecycle_consumer", source: lifecycleConsumer},
		},
		health:   healthServer,
		logger:   discardLogger(),
		interval: time.Millisecond,
		timeout:  time.Millisecond,
	}

	if ready, reason := monitor.check(context.Background()); ready || reason != "rabbitmq_dispatch_consumer" {
		t.Fatalf("initial readiness = %t/%q", ready, reason)
	}
	dispatchConsumer.ready = true
	if ready, reason := monitor.check(context.Background()); ready || reason != "rabbitmq_lifecycle_consumer" {
		t.Fatalf("lifecycle readiness = %t/%q", ready, reason)
	}
	lifecycleConsumer.ready = true
	database.err = errors.New("database unavailable")
	if ready, reason := monitor.check(context.Background()); ready || reason != "postgresql" {
		t.Fatalf("database readiness = %t/%q", ready, reason)
	}
	database.err = nil
	if ready, reason := monitor.check(context.Background()); !ready || reason != "dependencies_ready" {
		t.Fatalf("healthy readiness = %t/%q", ready, reason)
	}

	monitor.setServing(true, "test")
	response, err := healthServer.Check(context.Background(), &healthpb.HealthCheckRequest{Service: WorkerHealthService})
	if err != nil {
		t.Fatalf("health check: %v", err)
	}
	if response.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health status = %s", response.Status)
	}
}

type fakeReadinessSource struct{ ready bool }

func (s *fakeReadinessSource) Ready() bool { return s.ready }

type fakeDatabasePinger struct{ err error }

func (p *fakeDatabasePinger) Ping(context.Context) error { return p.err }
