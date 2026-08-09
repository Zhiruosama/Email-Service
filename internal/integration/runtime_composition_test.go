//go:build integration

package integration_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/db/migrations"
	"github.com/Zhiruosama/Email-Service/internal/application/delivery"
	"github.com/Zhiruosama/Email-Service/internal/bootstrap"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	postgresstore "github.com/Zhiruosama/Email-Service/internal/storage/postgres"
	"github.com/Zhiruosama/Email-Service/internal/testkit/postgrescontainer"
	"github.com/Zhiruosama/Email-Service/internal/testkit/rabbitmqcontainer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestRuntimeComposition(t *testing.T) {
	postgresInstance := postgrescontainer.StartInstance(t)
	rabbitInstance := rabbitmqcontainer.Start(t)
	config := runtimeIntegrationConfig(postgresInstance.ConnectionString, rabbitInstance.URL)

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := bootstrap.NewApp(startupCtx, config, discardIntegrationLogger())
	startupCancel()
	if !errors.Is(err, bootstrap.ErrStartup) {
		t.Fatalf("startup without migrations error = %v, want ErrStartup", err)
	}

	applyRuntimeMigrations(t, postgresInstance)
	applyConsumerReliabilityPolicy(t, rabbitInstance)
	fixturePool, err := pgxpool.New(context.Background(), postgresInstance.ConnectionString)
	if err != nil {
		t.Fatalf("create fixture pool: %v", err)
	}
	t.Cleanup(fixturePool.Close)

	const (
		tenantID  = "e0000000-0000-4000-8000-000000000001"
		messageID = "e1000000-0000-4000-8000-000000000001"
	)
	insertRepositoryTenant(t, context.Background(), fixturePool, tenantID, "runtime-composition")
	store := delivery.NewReliableMessageStore(postgresstore.NewTransactionManager(fixturePool))
	record := newRepositoryRecord(t, recordParams{
		messageID:      messageID,
		tenantID:       tenantID,
		idempotencyKey: "runtime-full-pipeline",
		fingerprint:    fingerprint(0xe1),
		now:            time.Now().UTC(),
	})
	if _, err := store.Create(context.Background(), record); err != nil {
		t.Fatalf("create runtime fixture: %v", err)
	}

	appCtx, cancelApp := context.WithCancel(context.Background())
	app, err := bootstrap.NewApp(appCtx, config, discardIntegrationLogger())
	if err != nil {
		cancelApp()
		t.Fatalf("create composed application: %v", err)
	}
	runResult := make(chan error, 1)
	go func() { runResult <- app.Run(appCtx) }()

	waitForRuntimeHealth(t, app.GRPCAddress(), bootstrap.WorkerHealthService, 10*time.Second)
	waitForRuntimeDelivery(t, fixturePool, messageID, 15*time.Second)

	cancelApp()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("runtime graceful shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runtime did not stop within shutdown bound")
	}
}

func runtimeIntegrationConfig(databaseURL, rabbitURL string) bootstrap.Config {
	config := bootstrap.DefaultConfig(databaseURL, rabbitURL, "runtime-integration", bootstrap.FakeProvider)
	config.GRPCListenAddress = "127.0.0.1:0"
	config.ShutdownTimeout = 6 * time.Second
	config.HealthInterval = 100 * time.Millisecond
	config.HealthTimeout = 100 * time.Millisecond
	config.Database.MinConnections = 0
	config.Database.MaxConnections = 6
	config.Database.ConnectTimeout = 2 * time.Second
	config.SchedulerBatchSize = 10
	config.SchedulerLoop = bootstrap.PollConfig{
		IdleDelay: 10 * time.Millisecond,
		ErrorBase: 10 * time.Millisecond,
		ErrorCap:  100 * time.Millisecond,
	}
	config.Relay.BatchSize = 10
	config.Relay.PublishConcurrency = 2
	config.Relay.LeaseDuration = 5 * time.Second
	config.Relay.PublishTimeout = 2 * time.Second
	config.RelayLoop = config.SchedulerLoop
	config.OutboxRetryBase = 10 * time.Millisecond
	config.OutboxRetryCap = time.Second
	config.Worker.ProviderTimeout = 2 * time.Second
	config.Worker.FinalizeTimeout = 2 * time.Second
	config.DeliveryRetryBase = 10 * time.Millisecond
	config.DeliveryRetryCap = time.Second
	config.Publisher.ChannelPoolSize = 2
	config.Consumer.LaneCount = 2
	config.Consumer.PrefetchPerLane = 1
	config.Consumer.ReconnectBase = 10 * time.Millisecond
	config.Consumer.ReconnectCap = 100 * time.Millisecond
	config.Consumer.ShutdownTimeout = 3 * time.Second
	return config
}

func applyRuntimeMigrations(t *testing.T, instance postgrescontainer.Instance) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	goose.SetBaseFS(migrations.Files)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("set migration dialect: %v", err)
	}
	if err := goose.UpContext(ctx, instance.SQL, "sql"); err != nil {
		t.Fatalf("apply runtime migrations: %v", err)
	}
}

func waitForRuntimeHealth(t *testing.T, address, service string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	connection, err := grpc.DialContext(
		ctx,
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial runtime health endpoint: %v", err)
	}
	defer connection.Close()
	client := healthpb.NewHealthClient(connection)
	for {
		response, checkErr := client.Check(ctx, &healthpb.HealthCheckRequest{Service: service})
		if checkErr == nil && response.Status == healthpb.HealthCheckResponse_SERVING {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("runtime health %q did not become SERVING: %v", service, checkErr)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func waitForRuntimeDelivery(
	t *testing.T,
	pool *pgxpool.Pool,
	messageID string,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		var attempts, pendingOutbox int
		err := pool.QueryRow(context.Background(), `
SELECT
    m.status,
    (SELECT count(*) FROM delivery_attempts a WHERE a.message_id = m.id),
    (SELECT count(*) FROM outbox_events o WHERE o.aggregate_id = m.id AND o.status = 'PENDING')
FROM mail_messages m
WHERE m.id = $1
`, messageID).Scan(&status, &attempts, &pendingOutbox)
		if err == nil && status == string(message.StatusProviderAccepted) && attempts == 1 && pendingOutbox == 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("message %s did not traverse Scheduler/Relay/Consumer/Fake Provider", messageID)
}

func discardIntegrationLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
