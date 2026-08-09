//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/delivery"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	providerfake "github.com/Zhiruosama/Email-Service/internal/provider/fake"
	postgresstore "github.com/Zhiruosama/Email-Service/internal/storage/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDispatchWorker(t *testing.T) {
	ctx := context.Background()
	pool, messageRepository := setupMessageRepository(t)
	transactor := postgresstore.NewTransactionManager(pool)
	store := delivery.NewReliableMessageStore(transactor)

	const tenantID = "90000000-0000-4000-8000-000000000001"
	insertRepositoryTenant(t, ctx, pool, tenantID, "dispatch-worker")

	t.Run("commits claim before provider and finalizes accepted result", func(t *testing.T) {
		record, command := createDispatchRecord(
			t,
			ctx,
			store,
			tenantID,
			"91000000-0000-4000-8000-000000000001",
			"92000000-0000-4000-8000-000000000001",
			"worker-accepted",
			0xd1,
		)
		provider := providerfake.New(func(
			_ context.Context,
			request ports.ProviderRequest,
		) ports.ProviderResult {
			var messageStatus, attemptStatus string
			if err := pool.QueryRow(ctx, `
				SELECT m.status, a.status
				FROM mail_messages m
				JOIN delivery_attempts a ON a.message_id = m.id
				WHERE m.id = $1 AND a.id = $2
			`, request.MessageID, request.AttemptID).Scan(&messageStatus, &attemptStatus); err != nil {
				t.Fatalf("observe committed provider boundary: %v", err)
			}
			if messageStatus != "SENDING" || attemptStatus != "STARTED" {
				t.Fatalf(
					"provider boundary = message:%s attempt:%s, want SENDING/STARTED",
					messageStatus,
					attemptStatus,
				)
			}
			return ports.ProviderResult{
				Outcome:           ports.ProviderOutcomeAccepted,
				ProviderMessageID: "fake-provider-message-1",
			}
		})
		worker := mustDispatchWorker(t, transactor, provider, fixedDeliveryRetry(30*time.Second))

		result, err := worker.Process(ctx, command)
		if err != nil {
			t.Fatalf("process accepted dispatch: %v", err)
		}
		if result.Disposition != delivery.DispatchProviderAccepted ||
			result.AttemptNumber != 1 || result.AttemptID == "" {
			t.Fatalf("accepted result = %#v", result)
		}

		persisted, err := messageRepository.GetByID(ctx, record.Message.ID())
		if err != nil {
			t.Fatalf("load accepted message: %v", err)
		}
		if persisted.Message.Status() != message.StatusProviderAccepted ||
			persisted.Message.Version() != 2 ||
			persisted.Message.Snapshot().ProviderMessageID != "fake-provider-message-1" {
			t.Fatalf("accepted snapshot = %#v", persisted.Message.Snapshot())
		}
		assertDeliveryAttempt(
			t,
			ctx,
			pool,
			result.AttemptID,
			"PROVIDER_ACCEPTED",
			"fake-provider-message-1",
			"",
		)
		if countOutboxEvents(t, ctx, pool, record.Message.ID()) != 5 {
			t.Fatal("accepted dispatch did not atomically append both status events")
		}

		duplicate, err := worker.Process(ctx, command)
		if err != nil {
			t.Fatalf("process duplicate dispatch: %v", err)
		}
		if duplicate.Disposition != delivery.DispatchStale {
			t.Fatalf("duplicate disposition = %q, want STALE", duplicate.Disposition)
		}
		if len(provider.Requests()) != 1 {
			t.Fatalf("provider calls = %d, want one", len(provider.Requests()))
		}
	})

	t.Run("schedules a retry for a known retryable failure", func(t *testing.T) {
		record, command := createDispatchRecord(
			t,
			ctx,
			store,
			tenantID,
			"91000000-0000-4000-8000-000000000002",
			"92000000-0000-4000-8000-000000000002",
			"worker-retry",
			0xd2,
		)
		failure := message.Failure{
			Category:  message.FailureRateLimited,
			Code:      "PROVIDER_RATE_LIMITED",
			Retryable: true,
		}
		provider := providerfake.New(func(context.Context, ports.ProviderRequest) ports.ProviderResult {
			return ports.ProviderResult{Outcome: ports.ProviderOutcomeFailed, Failure: &failure}
		})
		worker := mustDispatchWorker(t, transactor, provider, fixedDeliveryRetry(time.Minute))

		result, err := worker.Process(ctx, command)
		if err != nil {
			t.Fatalf("process retryable dispatch: %v", err)
		}
		if result.Disposition != delivery.DispatchRetryScheduled {
			t.Fatalf("retry result = %#v", result)
		}
		persisted, err := messageRepository.GetByID(ctx, record.Message.ID())
		if err != nil {
			t.Fatalf("load retry message: %v", err)
		}
		if persisted.Message.Status() != message.StatusRetryScheduled ||
			persisted.Message.NextAttemptAt() == nil {
			t.Fatalf("retry snapshot = %#v", persisted.Message.Snapshot())
		}
		assertDeliveryAttempt(
			t,
			ctx,
			pool,
			result.AttemptID,
			"FAILED",
			"",
			"PROVIDER_RATE_LIMITED",
		)
	})

	t.Run("records an ambiguous submission without retrying", func(t *testing.T) {
		record, command := createDispatchRecord(
			t,
			ctx,
			store,
			tenantID,
			"91000000-0000-4000-8000-000000000003",
			"92000000-0000-4000-8000-000000000003",
			"worker-unknown",
			0xd3,
		)
		failure := message.Failure{
			Category:  message.FailureSubmissionUnknown,
			Code:      "SMTP_DATA_RESULT_UNKNOWN",
			Retryable: false,
		}
		provider := providerfake.New(func(context.Context, ports.ProviderRequest) ports.ProviderResult {
			return ports.ProviderResult{
				Outcome: ports.ProviderOutcomeSubmissionUnknown,
				Failure: &failure,
			}
		})
		worker := mustDispatchWorker(t, transactor, provider, fixedDeliveryRetry(time.Minute))

		result, err := worker.Process(ctx, command)
		if err != nil {
			t.Fatalf("process ambiguous dispatch: %v", err)
		}
		if result.Disposition != delivery.DispatchSubmissionUnknown {
			t.Fatalf("unknown result = %#v", result)
		}
		assertPersistedMessageState(
			t,
			ctx,
			messageRepository,
			record.Message.ID(),
			message.StatusSubmissionUnknown,
			2,
		)
		assertDeliveryAttempt(
			t,
			ctx,
			pool,
			result.AttemptID,
			"SUBMISSION_UNKNOWN",
			"",
			"SMTP_DATA_RESULT_UNKNOWN",
		)
	})

	t.Run("schedules retry when delivery material is temporarily unavailable", func(t *testing.T) {
		record, command := createDispatchRecord(
			t,
			ctx,
			store,
			tenantID,
			"91000000-0000-4000-8000-000000000007",
			"92000000-0000-4000-8000-000000000007",
			"worker-material-retry",
			0xd7,
		)
		provider := providerfake.New(nil)
		worker := mustDispatchWorkerWithMaterial(
			t,
			transactor,
			provider,
			failingIntegrationMaterialBuilder{
				err: ports.NewDeliveryMaterialError(
					"PAYLOAD_KEY_UNAVAILABLE",
					true,
					errors.New("private key-service detail must not persist"),
				),
			},
			fixedDeliveryRetry(time.Minute),
		)

		result, err := worker.Process(ctx, command)
		if err != nil {
			t.Fatalf("process retryable material failure: %v", err)
		}
		if result.Disposition != delivery.DispatchRetryScheduled {
			t.Fatalf("material retry result = %#v", result)
		}
		assertPersistedMessageState(
			t,
			ctx,
			messageRepository,
			record.Message.ID(),
			message.StatusRetryScheduled,
			2,
		)
		assertDeliveryAttempt(
			t,
			ctx,
			pool,
			result.AttemptID,
			"FAILED",
			"",
			"PAYLOAD_KEY_UNAVAILABLE",
		)
		if len(provider.Requests()) != 0 {
			t.Fatal("provider was called without delivery material")
		}
	})

	t.Run("permanently fails authenticated material corruption", func(t *testing.T) {
		record, command := createDispatchRecord(
			t,
			ctx,
			store,
			tenantID,
			"91000000-0000-4000-8000-000000000008",
			"92000000-0000-4000-8000-000000000008",
			"worker-material-permanent",
			0xd8,
		)
		provider := providerfake.New(nil)
		worker := mustDispatchWorkerWithMaterial(
			t,
			transactor,
			provider,
			failingIntegrationMaterialBuilder{
				err: ports.NewDeliveryMaterialError(
					"PAYLOAD_AUTHENTICATION_FAILED",
					false,
					errors.New("private ciphertext detail must not persist"),
				),
			},
			fixedDeliveryRetry(time.Minute),
		)

		result, err := worker.Process(ctx, command)
		if err != nil {
			t.Fatalf("process permanent material failure: %v", err)
		}
		if result.Disposition != delivery.DispatchPermanentlyFailed {
			t.Fatalf("material permanent result = %#v", result)
		}
		assertPersistedMessageState(
			t,
			ctx,
			messageRepository,
			record.Message.ID(),
			message.StatusPermanentlyFailed,
			2,
		)
		assertDeliveryAttempt(
			t,
			ctx,
			pool,
			result.AttemptID,
			"FAILED",
			"",
			"PAYLOAD_AUTHENTICATION_FAILED",
		)
		if len(provider.Requests()) != 0 {
			t.Fatal("provider was called with corrupted delivery material")
		}
	})

	t.Run("rolls back claim if its outbox event cannot persist", func(t *testing.T) {
		record, command := createDispatchRecord(
			t,
			ctx,
			store,
			tenantID,
			"91000000-0000-4000-8000-000000000004",
			"92000000-0000-4000-8000-000000000004",
			"worker-claim-rollback",
			0xd4,
		)
		provider := providerfake.New(nil)
		worker := mustDispatchWorker(t, transactor, provider, fixedDeliveryRetry(time.Minute))
		installRejectingOutboxTrigger(t, ctx, pool)

		if _, err := worker.Process(ctx, command); !errors.Is(err, ports.ErrOutboxRepository) {
			t.Fatalf("claim rollback error = %v, want ErrOutboxRepository", err)
		}
		assertPersistedMessageState(
			t,
			ctx,
			messageRepository,
			record.Message.ID(),
			message.StatusQueued,
			0,
		)
		if countDeliveryAttempts(t, ctx, pool, record.Message.ID()) != 0 {
			t.Fatal("attempt survived a rolled-back claim")
		}
		if len(provider.Requests()) != 0 {
			t.Fatal("provider ran before the claim transaction committed")
		}
	})

	t.Run("keeps a visible started attempt if finalization rolls back", func(t *testing.T) {
		record, command := createDispatchRecord(
			t,
			ctx,
			store,
			tenantID,
			"91000000-0000-4000-8000-000000000005",
			"92000000-0000-4000-8000-000000000005",
			"worker-finalize-rollback",
			0xd5,
		)
		provider := providerfake.New(func(context.Context, ports.ProviderRequest) ports.ProviderResult {
			installRejectingOutboxTrigger(t, ctx, pool)
			return ports.ProviderResult{
				Outcome:           ports.ProviderOutcomeAccepted,
				ProviderMessageID: "accepted-before-db-failure",
			}
		})
		worker := mustDispatchWorker(t, transactor, provider, fixedDeliveryRetry(time.Minute))

		if _, err := worker.Process(ctx, command); !errors.Is(err, ports.ErrOutboxRepository) {
			t.Fatalf("finalize rollback error = %v, want ErrOutboxRepository", err)
		}
		assertPersistedMessageState(
			t,
			ctx,
			messageRepository,
			record.Message.ID(),
			message.StatusSending,
			1,
		)
		assertSingleStartedAttempt(t, ctx, pool, record.Message.ID())
	})

	t.Run("concurrent duplicate invokes provider only once", func(t *testing.T) {
		_, command := createDispatchRecord(
			t,
			ctx,
			store,
			tenantID,
			"91000000-0000-4000-8000-000000000006",
			"92000000-0000-4000-8000-000000000006",
			"worker-concurrent-duplicate",
			0xd6,
		)
		providerStarted := make(chan struct{})
		releaseProvider := make(chan struct{})
		provider := providerfake.New(func(context.Context, ports.ProviderRequest) ports.ProviderResult {
			close(providerStarted)
			<-releaseProvider
			return ports.ProviderResult{
				Outcome:           ports.ProviderOutcomeAccepted,
				ProviderMessageID: "concurrent-provider-id",
			}
		})
		worker := mustDispatchWorker(t, transactor, provider, fixedDeliveryRetry(time.Minute))

		type processOutcome struct {
			result delivery.DispatchResult
			err    error
		}
		firstDone := make(chan processOutcome, 1)
		go func() {
			result, err := worker.Process(ctx, command)
			firstDone <- processOutcome{result: result, err: err}
		}()
		<-providerStarted

		duplicate, err := worker.Process(ctx, command)
		if err != nil {
			close(releaseProvider)
			t.Fatalf("concurrent duplicate: %v", err)
		}
		if duplicate.Disposition != delivery.DispatchStale {
			close(releaseProvider)
			t.Fatalf("concurrent duplicate = %#v, want STALE", duplicate)
		}
		close(releaseProvider)
		first := <-firstDone
		if first.err != nil || first.result.Disposition != delivery.DispatchProviderAccepted {
			t.Fatalf("first concurrent result = %#v, error = %v", first.result, first.err)
		}
		if len(provider.Requests()) != 1 {
			t.Fatalf("concurrent provider calls = %d, want one", len(provider.Requests()))
		}
	})
}

type fixedDeliveryRetry time.Duration

func (f fixedDeliveryRetry) NextDelay(uint32) time.Duration { return time.Duration(f) }

func mustDispatchWorker(
	t *testing.T,
	transactor ports.Transactor,
	provider ports.EmailProvider,
	retry delivery.DeliveryRetryPolicy,
) *delivery.DispatchWorker {
	return mustDispatchWorkerWithMaterial(
		t,
		transactor,
		provider,
		integrationMaterialBuilder{},
		retry,
	)
}

func mustDispatchWorkerWithMaterial(
	t *testing.T,
	transactor ports.Transactor,
	provider ports.EmailProvider,
	materialBuilder ports.DeliveryMaterialBuilder,
	retry delivery.DeliveryRetryPolicy,
) *delivery.DispatchWorker {
	t.Helper()
	worker, err := delivery.NewDispatchWorker(
		transactor,
		provider,
		materialBuilder,
		retry,
		delivery.DispatchWorkerConfig{
			ProviderTimeout: 5 * time.Second,
			FinalizeTimeout: 5 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("new Dispatch Worker: %v", err)
	}
	return worker
}

type failingIntegrationMaterialBuilder struct {
	err error
}

func (b failingIntegrationMaterialBuilder) Build(
	context.Context,
	ports.MessageRecord,
	ports.StartedDeliveryAttempt,
) (ports.DeliveryMaterial, error) {
	return ports.DeliveryMaterial{}, b.err
}

type integrationMaterialBuilder struct{}

func (integrationMaterialBuilder) Build(
	_ context.Context,
	record ports.MessageRecord,
	_ ports.StartedDeliveryAttempt,
) (ports.DeliveryMaterial, error) {
	return ports.DeliveryMaterial{
		EnvelopeFrom: "sender@example.com",
		EnvelopeTo:   "recipient@example.com",
		MIMEMessage: []byte(
			"From: sender@example.com\r\n" +
				"To: recipient@example.com\r\n" +
				"Subject: integration test\r\n\r\n" +
				"message " + record.Message.ID(),
		),
	}, nil
}

func createDispatchRecord(
	t *testing.T,
	ctx context.Context,
	store *delivery.ReliableMessageStore,
	tenantID string,
	messageID string,
	eventID string,
	idempotencyKey string,
	fingerprintByte byte,
) (ports.MessageRecord, delivery.DispatchCommand) {
	t.Helper()
	now := time.Now().UTC()
	record := newRepositoryRecord(t, recordParams{
		messageID:      messageID,
		tenantID:       tenantID,
		idempotencyKey: idempotencyKey,
		fingerprint:    fingerprint(fingerprintByte),
		now:            now,
	})
	if _, err := store.Create(ctx, record); err != nil {
		t.Fatalf("create dispatch record: %v", err)
	}
	return record, delivery.DispatchCommand{
		EventID:            eventID,
		TenantID:           tenantID,
		MessageID:          messageID,
		AggregateSequence:  record.Message.LatestSequence(),
		DispatchGeneration: record.Message.DispatchGeneration(),
	}
}

func assertDeliveryAttempt(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	attemptID string,
	wantStatus string,
	wantProviderMessageID string,
	wantErrorCode string,
) {
	t.Helper()
	var status string
	var providerMessageID, errorCode *string
	var startedAt time.Time
	var finishedAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT status, started_at, finished_at, provider_message_id, error_code
		FROM delivery_attempts
		WHERE id = $1
	`, attemptID).Scan(
		&status,
		&startedAt,
		&finishedAt,
		&providerMessageID,
		&errorCode,
	); err != nil {
		t.Fatalf("query delivery attempt: %v", err)
	}
	if status != wantStatus || finishedAt == nil || finishedAt.Before(startedAt) {
		t.Fatalf("attempt status/timestamps = %s/%v/%v", status, startedAt, finishedAt)
	}
	if stringValue(providerMessageID) != wantProviderMessageID || stringValue(errorCode) != wantErrorCode {
		t.Fatalf(
			"attempt provider id/error = %q/%q, want %q/%q",
			stringValue(providerMessageID),
			stringValue(errorCode),
			wantProviderMessageID,
			wantErrorCode,
		)
	}
}

func assertSingleStartedAttempt(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	messageID string,
) {
	t.Helper()
	var count int
	var minimumStatus, maximumStatus string
	if err := pool.QueryRow(ctx, `
		SELECT count(*), min(status), max(status)
		FROM delivery_attempts
		WHERE message_id = $1
	`, messageID).Scan(&count, &minimumStatus, &maximumStatus); err != nil {
		t.Fatalf("query started attempt: %v", err)
	}
	if count != 1 || minimumStatus != "STARTED" || maximumStatus != "STARTED" {
		t.Fatalf("attempt count/status = %d/%s/%s, want one STARTED", count, minimumStatus, maximumStatus)
	}
}

func countDeliveryAttempts(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	messageID string,
) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(
		ctx,
		"SELECT count(*) FROM delivery_attempts WHERE message_id = $1",
		messageID,
	).Scan(&count); err != nil {
		t.Fatalf("count delivery attempts: %v", err)
	}
	return count
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
