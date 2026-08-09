//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/db/migrations"
	deliveryv1 "github.com/Zhiruosama/Email-Service/gen/go/mailservice/delivery/v1"
	"github.com/Zhiruosama/Email-Service/internal/bootstrap"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"github.com/Zhiruosama/Email-Service/internal/testkit/postgrescontainer"
	"github.com/Zhiruosama/Email-Service/internal/testkit/rabbitmqcontainer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRuntimeComposition(t *testing.T) {
	postgresInstance := postgrescontainer.StartInstance(t)
	rabbitInstance := rabbitmqcontainer.Start(t)
	callbackAddress, callbackReceiver := startRuntimeCallbackServer(t)
	const tenantID = "e0000000-0000-4000-8000-000000000001"
	config := runtimeIntegrationConfig(postgresInstance.ConnectionString, rabbitInstance.URL)
	config.SubmissionSecurity.DevelopmentTenantID = tenantID
	config.Callback.GRPC.Address = callbackAddress

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

	insertRepositoryTenant(t, context.Background(), fixturePool, tenantID, "runtime-composition")

	appCtx, cancelApp := context.WithCancel(context.Background())
	app, err := bootstrap.NewApp(appCtx, config, discardIntegrationLogger())
	if err != nil {
		cancelApp()
		t.Fatalf("create composed application: %v", err)
	}
	runResult := make(chan error, 1)
	go func() { runResult <- app.Run(appCtx) }()

	waitForRuntimeHealth(t, app.GRPCAddress(), bootstrap.WorkerHealthService, 10*time.Second)
	connection, err := grpc.NewClient(
		app.GRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		cancelApp()
		t.Fatalf("dial delivery service: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	variables, err := structpb.NewStruct(map[string]any{
		"code":              "123456",
		"purpose":           "LOGIN",
		"valid_for_seconds": 300,
	})
	if err != nil {
		cancelApp()
		t.Fatalf("create runtime variables: %v", err)
	}
	request := &deliveryv1.SubmitEmailRequest{
		IdempotencyKey:    "runtime-full-pipeline",
		Recipient:         &deliveryv1.Recipient{Email: "runtime@example.com"},
		SenderIdentityKey: "ainexus.default",
		Content: &deliveryv1.EmailContent{
			Template:  &deliveryv1.TemplateReference{Key: "verification_code.v1"},
			Locale:    "zh-CN",
			Variables: variables,
		},
		Category:            deliveryv1.EmailCategory_EMAIL_CATEGORY_CRITICAL,
		Priority:            9,
		DispatchDeadline:    timestamppb.New(time.Now().UTC().Add(2 * time.Minute)),
		DuplicateRiskPolicy: deliveryv1.DuplicateRiskPolicy_DUPLICATE_RISK_POLICY_AVOID_DUPLICATE,
	}
	rpcCtx, cancelRPC := context.WithTimeout(context.Background(), 5*time.Second)
	submitted, err := deliveryv1.NewDeliveryServiceClient(connection).SubmitEmail(rpcCtx, request)
	cancelRPC()
	if err != nil {
		cancelApp()
		t.Fatalf("submit through gRPC: %v", err)
	}
	if submitted.Disposition != deliveryv1.SubmitDisposition_SUBMIT_DISPOSITION_ACCEPTED {
		cancelApp()
		t.Fatalf("submit disposition = %s, want ACCEPTED", submitted.Disposition)
	}
	messageID := submitted.Message.MessageId
	waitForRuntimeDelivery(t, fixturePool, messageID, 15*time.Second)
	waitForRuntimeCallbacks(t, callbackReceiver, messageID, 15*time.Second)

	queryCtx, cancelQuery := context.WithTimeout(context.Background(), 5*time.Second)
	queried, err := deliveryv1.NewDeliveryServiceClient(connection).GetEmail(
		queryCtx,
		&deliveryv1.GetEmailRequest{
			Selector: &deliveryv1.GetEmailRequest_IdempotencyKey{
				IdempotencyKey: "runtime-full-pipeline",
			},
		},
	)
	cancelQuery()
	if err != nil {
		cancelApp()
		t.Fatalf("query through gRPC: %v", err)
	}
	if queried.Message.MessageId != messageID ||
		queried.Message.Status != deliveryv1.DeliveryStatus_DELIVERY_STATUS_PROVIDER_ACCEPTED ||
		queried.Message.Recipient.MaskedEmail != "r***@example.com" {
		cancelApp()
		t.Fatalf("unexpected queried message: %#v", queried.Message)
	}

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
	config.SubmissionSecurity = bootstrap.SubmissionSecurityConfig{
		AllowInsecureGRPC:   true,
		DevelopmentTenantID: "e0000000-0000-4000-8000-000000000001",
		PayloadKeyID:        "runtime-integration-key",
		EncryptionKey:       bytes.Repeat([]byte{0x31}, 32),
		FingerprintKey:      bytes.Repeat([]byte{0x42}, 32),
	}
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
	config.NotificationWorker.CallbackTimeout = 2 * time.Second
	config.Callback.AllowInsecure = true
	config.LifecycleConsumer.LaneCount = 2
	config.LifecycleConsumer.PrefetchPerLane = 1
	config.LifecycleConsumer.ReconnectBase = 10 * time.Millisecond
	config.LifecycleConsumer.ReconnectCap = 100 * time.Millisecond
	config.LifecycleConsumer.TransientRequeueBase = time.Millisecond
	config.LifecycleConsumer.TransientRequeueCap = time.Millisecond
	config.LifecycleConsumer.ShutdownTimeout = 3 * time.Second
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
	var lastObservation string
	for time.Now().Before(deadline) {
		var status string
		var attempts, pendingOutbox, deliveryEvents int
		err := pool.QueryRow(context.Background(), `
SELECT
    m.status,
    (SELECT count(*) FROM delivery_attempts a WHERE a.message_id = m.id),
    (SELECT count(*) FROM outbox_events o WHERE o.aggregate_id = m.id AND o.status = 'PENDING'),
    (SELECT count(*) FROM delivery_events e WHERE e.message_id = m.id)
FROM mail_messages m
WHERE m.id = $1
`, messageID).Scan(&status, &attempts, &pendingOutbox, &deliveryEvents)
		if err == nil && status == string(message.StatusProviderAccepted) &&
			attempts == 1 && pendingOutbox == 0 && deliveryEvents == 4 {
			return
		}
		if err == nil {
			lastObservation = fmt.Sprintf(
				"status=%s attempts=%d pending_outbox=%d delivery_events=%d",
				status,
				attempts,
				pendingOutbox,
				deliveryEvents,
			)
		} else {
			lastObservation = "query_error=" + err.Error()
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf(
		"message %s did not traverse Scheduler/Relay/Consumer/Fake Provider: %s",
		messageID,
		lastObservation,
	)
}

type runtimeCallbackObservation struct {
	eventID        string
	messageID      string
	idempotencyKey string
	status         deliveryv1.DeliveryStatus
	sequence       uint64
}

type runtimeCallbackReceiver struct {
	deliveryv1.UnimplementedDeliveryEventReceiverServiceServer
	mu           sync.Mutex
	seen         map[string]struct{}
	latest       map[string]uint64
	observations map[string]runtimeCallbackObservation
}

func startRuntimeCallbackServer(t *testing.T) (string, *runtimeCallbackReceiver) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for runtime callback: %v", err)
	}
	receiver := &runtimeCallbackReceiver{
		seen:         make(map[string]struct{}),
		latest:       make(map[string]uint64),
		observations: make(map[string]runtimeCallbackObservation),
	}
	server := grpc.NewServer()
	deliveryv1.RegisterDeliveryEventReceiverServiceServer(server, receiver)
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-serveResult
	})
	return listener.Addr().String(), receiver
}

func (r *runtimeCallbackReceiver) ReportDeliveryEvent(
	_ context.Context,
	request *deliveryv1.ReportDeliveryEventRequest,
) (*deliveryv1.ReportDeliveryEventResponse, error) {
	event := request.GetEvent()
	r.mu.Lock()
	defer r.mu.Unlock()
	disposition := deliveryv1.EventAckDisposition_EVENT_ACK_DISPOSITION_ACCEPTED
	if _, duplicate := r.seen[event.GetEventId()]; duplicate {
		disposition = deliveryv1.EventAckDisposition_EVENT_ACK_DISPOSITION_DUPLICATE
	} else {
		r.seen[event.GetEventId()] = struct{}{}
		if event.GetSequence() <= r.latest[event.GetMessageId()] {
			disposition = deliveryv1.EventAckDisposition_EVENT_ACK_DISPOSITION_IGNORED_STALE
		} else {
			r.latest[event.GetMessageId()] = event.GetSequence()
		}
		r.observations[event.GetEventId()] = runtimeCallbackObservation{
			eventID:        event.GetEventId(),
			messageID:      event.GetMessageId(),
			idempotencyKey: event.GetIdempotencyKey(),
			status:         event.GetStatus(),
			sequence:       event.GetSequence(),
		}
	}
	return &deliveryv1.ReportDeliveryEventResponse{
		EventId:     event.GetEventId(),
		Disposition: disposition,
	}, nil
}

func waitForRuntimeCallbacks(
	t *testing.T,
	receiver *runtimeCallbackReceiver,
	messageID string,
	timeout time.Duration,
) {
	t.Helper()
	wantStatuses := map[uint64]deliveryv1.DeliveryStatus{
		1: deliveryv1.DeliveryStatus_DELIVERY_STATUS_ACCEPTED,
		2: deliveryv1.DeliveryStatus_DELIVERY_STATUS_QUEUED,
		3: deliveryv1.DeliveryStatus_DELIVERY_STATUS_SENDING,
		4: deliveryv1.DeliveryStatus_DELIVERY_STATUS_PROVIDER_ACCEPTED,
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		receiver.mu.Lock()
		observed := make(map[uint64]runtimeCallbackObservation)
		for _, observation := range receiver.observations {
			if observation.messageID == messageID {
				observed[observation.sequence] = observation
			}
		}
		receiver.mu.Unlock()
		if len(observed) == len(wantStatuses) {
			for sequence, wantStatus := range wantStatuses {
				observation, ok := observed[sequence]
				if !ok || observation.eventID == "" ||
					observation.idempotencyKey != "runtime-full-pipeline" ||
					observation.status != wantStatus {
					t.Fatalf("callback sequence %d = %#v, want status %s", sequence, observation, wantStatus)
				}
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("message %s did not deliver all lifecycle callbacks", messageID)
}

func discardIntegrationLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
