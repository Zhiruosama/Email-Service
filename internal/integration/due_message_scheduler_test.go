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
	postgresstore "github.com/Zhiruosama/Email-Service/internal/storage/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDueMessageScheduler(t *testing.T) {
	ctx := context.Background()
	pool, messageRepository := setupMessageRepository(t)
	transactor := postgresstore.NewTransactionManager(pool)
	store := delivery.NewReliableMessageStore(transactor)
	scheduler := mustDueMessageScheduler(t, transactor, 10)

	const tenantID = "82000000-0000-4000-8000-000000000001"
	insertRepositoryTenant(t, ctx, pool, tenantID, "due-message-scheduler")
	var runAt time.Time
	if err := pool.QueryRow(ctx, "SELECT transaction_timestamp()").Scan(&runAt); err != nil {
		t.Fatalf("read PostgreSQL clock: %v", err)
	}
	runAt = runAt.UTC()

	t.Run("queues scheduled and retry messages and expires overdue messages", func(t *testing.T) {
		scheduled := createScheduledRecord(t, ctx, store, scheduledRecordParams{
			messageID:      "83000000-0000-4000-8000-000000000001",
			tenantID:       tenantID,
			idempotencyKey: "scheduler-due",
			fingerprint:    fingerprint(0xa1),
			acceptedAt:     runAt.Add(-5 * time.Minute),
			scheduledAt:    runAt.Add(-time.Minute),
		})
		overdue := createScheduledRecord(t, ctx, store, scheduledRecordParams{
			messageID:      "83000000-0000-4000-8000-000000000002",
			tenantID:       tenantID,
			idempotencyKey: "scheduler-overdue",
			fingerprint:    fingerprint(0xa2),
			acceptedAt:     runAt.Add(-20 * time.Minute),
			scheduledAt:    runAt.Add(-19 * time.Minute),
		})
		future := createScheduledRecord(t, ctx, store, scheduledRecordParams{
			messageID:      "83000000-0000-4000-8000-000000000003",
			tenantID:       tenantID,
			idempotencyKey: "scheduler-future",
			fingerprint:    fingerprint(0xa3),
			acceptedAt:     runAt.Add(-time.Minute),
			scheduledAt:    runAt.Add(5 * time.Minute),
		})
		retry := createDueRetryRecord(
			t,
			ctx,
			store,
			tenantID,
			"83000000-0000-4000-8000-000000000004",
			runAt,
		)

		result, err := scheduler.RunOnce(ctx)
		if err != nil {
			t.Fatalf("run Scheduler: %v", err)
		}
		if result != (delivery.SchedulerBatchResult{Claimed: 3, Queued: 2, Expired: 1}) {
			t.Fatalf("Scheduler result = %#v, want claimed=3 queued=2 expired=1", result)
		}

		assertPersistedMessageState(t, ctx, messageRepository, scheduled.Message.ID(), message.StatusQueued, 1)
		assertPersistedMessageState(t, ctx, messageRepository, overdue.Message.ID(), message.StatusExpired, 1)
		assertPersistedMessageState(t, ctx, messageRepository, future.Message.ID(), message.StatusScheduled, 0)
		assertPersistedMessageState(t, ctx, messageRepository, retry.Message.ID(), message.StatusQueued, 2)

		assertOutboxCounts(t, ctx, pool, scheduled.Message.ID(), 4, 1)
		assertOutboxCounts(t, ctx, pool, overdue.Message.ID(), 3, 0)
		assertOutboxCounts(t, ctx, pool, future.Message.ID(), 2, 0)
		assertOutboxCounts(t, ctx, pool, retry.Message.ID(), 7, 2)

		empty, err := scheduler.RunOnce(ctx)
		if err != nil {
			t.Fatalf("run empty Scheduler batch: %v", err)
		}
		if empty != (delivery.SchedulerBatchResult{}) {
			t.Fatalf("empty Scheduler result = %#v", empty)
		}
	})

	rollbackMessageID := "84000000-0000-4000-8000-000000000001"
	t.Run("rolls back state when outbox write fails", func(t *testing.T) {
		record := createScheduledRecord(t, ctx, store, scheduledRecordParams{
			messageID:      rollbackMessageID,
			tenantID:       tenantID,
			idempotencyKey: "scheduler-rollback",
			fingerprint:    fingerprint(0xb1),
			acceptedAt:     runAt.Add(-4 * time.Minute),
			scheduledAt:    runAt.Add(-time.Minute),
		})
		installRejectingOutboxTrigger(t, ctx, pool)

		if _, err := scheduler.RunOnce(ctx); !errors.Is(err, ports.ErrOutboxRepository) {
			t.Fatalf("Scheduler error = %v, want ErrOutboxRepository", err)
		}
		assertPersistedMessageState(t, ctx, messageRepository, record.Message.ID(), message.StatusScheduled, 0)
		assertOutboxCounts(t, ctx, pool, record.Message.ID(), 2, 0)
	})

	t.Run("retries a rolled back due message", func(t *testing.T) {
		result, err := scheduler.RunOnce(ctx)
		if err != nil {
			t.Fatalf("retry Scheduler after rollback: %v", err)
		}
		if result != (delivery.SchedulerBatchResult{Claimed: 1, Queued: 1}) {
			t.Fatalf("retry result = %#v, want one queued", result)
		}
		assertPersistedMessageState(t, ctx, messageRepository, rollbackMessageID, message.StatusQueued, 1)
		assertOutboxCounts(t, ctx, pool, rollbackMessageID, 4, 1)
	})

	t.Run("leaves a paused tenant scheduled until the tenant is active", func(t *testing.T) {
		const pausedTenantID = "82000000-0000-4000-8000-000000000002"
		insertRepositoryTenant(t, ctx, pool, pausedTenantID, "paused-scheduler-tenant")
		record := createScheduledRecord(t, ctx, store, scheduledRecordParams{
			messageID:      "84000000-0000-4000-8000-000000000002",
			tenantID:       pausedTenantID,
			idempotencyKey: "scheduler-paused-tenant",
			fingerprint:    fingerprint(0xb2),
			acceptedAt:     runAt.Add(-4 * time.Minute),
			scheduledAt:    runAt.Add(-time.Minute),
		})
		if _, err := pool.Exec(
			ctx,
			"UPDATE tenants SET status = 'PAUSED', updated_at = CURRENT_TIMESTAMP WHERE id = $1",
			pausedTenantID,
		); err != nil {
			t.Fatalf("pause Scheduler tenant: %v", err)
		}

		pausedResult, err := scheduler.RunOnce(ctx)
		if err != nil {
			t.Fatalf("run Scheduler with paused tenant: %v", err)
		}
		if pausedResult != (delivery.SchedulerBatchResult{}) {
			t.Fatalf("paused tenant result = %#v, want empty batch", pausedResult)
		}
		assertPersistedMessageState(t, ctx, messageRepository, record.Message.ID(), message.StatusScheduled, 0)
		assertOutboxCounts(t, ctx, pool, record.Message.ID(), 2, 0)

		if _, err := pool.Exec(
			ctx,
			"UPDATE tenants SET status = 'ACTIVE', updated_at = CURRENT_TIMESTAMP WHERE id = $1",
			pausedTenantID,
		); err != nil {
			t.Fatalf("activate Scheduler tenant: %v", err)
		}
		activeResult, err := scheduler.RunOnce(ctx)
		if err != nil {
			t.Fatalf("run Scheduler after tenant activation: %v", err)
		}
		if activeResult != (delivery.SchedulerBatchResult{Claimed: 1, Queued: 1}) {
			t.Fatalf("activated tenant result = %#v, want one queued", activeResult)
		}
		assertPersistedMessageState(t, ctx, messageRepository, record.Message.ID(), message.StatusQueued, 1)
		assertOutboxCounts(t, ctx, pool, record.Message.ID(), 4, 1)
	})

	t.Run("skips a row locked by another Scheduler and recovers it after rollback", func(t *testing.T) {
		first := createScheduledRecord(t, ctx, store, scheduledRecordParams{
			messageID:      "85000000-0000-4000-8000-000000000001",
			tenantID:       tenantID,
			idempotencyKey: "scheduler-locked-first",
			fingerprint:    fingerprint(0xc1),
			acceptedAt:     runAt.Add(-6 * time.Minute),
			scheduledAt:    runAt.Add(-2 * time.Minute),
		})
		second := createScheduledRecord(t, ctx, store, scheduledRecordParams{
			messageID:      "85000000-0000-4000-8000-000000000002",
			tenantID:       tenantID,
			idempotencyKey: "scheduler-unlocked-second",
			fingerprint:    fingerprint(0xc2),
			acceptedAt:     runAt.Add(-5 * time.Minute),
			scheduledAt:    runAt.Add(-time.Minute),
		})
		third := createScheduledRecord(t, ctx, store, scheduledRecordParams{
			messageID:      "85000000-0000-4000-8000-000000000003",
			tenantID:       tenantID,
			idempotencyKey: "scheduler-batch-limited-third",
			fingerprint:    fingerprint(0xc3),
			acceptedAt:     runAt.Add(-4 * time.Minute),
			scheduledAt:    runAt.Add(-30 * time.Second),
		})

		blockingTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin blocking transaction: %v", err)
		}
		locked, err := postgresstore.NewDueMessageRepository(blockingTx).LockDue(
			ctx,
			ports.DueMessageQuery{Limit: 1},
		)
		if err != nil {
			_ = blockingTx.Rollback(ctx)
			t.Fatalf("lock first due message: %v", err)
		}
		if len(locked.Records) != 1 || locked.Records[0].Message.ID() != first.Message.ID() {
			_ = blockingTx.Rollback(ctx)
			t.Fatalf("locked records = %#v, want first message", locked)
		}

		skipCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		limitedScheduler := mustDueMessageScheduler(t, transactor, 1)
		skippedResult, err := limitedScheduler.RunOnce(skipCtx)
		if err != nil {
			_ = blockingTx.Rollback(ctx)
			t.Fatalf("run Scheduler while first row locked: %v", err)
		}
		if skippedResult != (delivery.SchedulerBatchResult{Claimed: 1, Queued: 1}) {
			_ = blockingTx.Rollback(ctx)
			t.Fatalf("skip-locked result = %#v, want second message queued", skippedResult)
		}
		assertPersistedMessageState(t, ctx, messageRepository, first.Message.ID(), message.StatusScheduled, 0)
		assertPersistedMessageState(t, ctx, messageRepository, second.Message.ID(), message.StatusQueued, 1)
		assertPersistedMessageState(t, ctx, messageRepository, third.Message.ID(), message.StatusScheduled, 0)

		if err := blockingTx.Rollback(ctx); err != nil {
			t.Fatalf("release first Scheduler lock: %v", err)
		}
		recoveredResult, err := scheduler.RunOnce(ctx)
		if err != nil {
			t.Fatalf("recover rolled-back locked message: %v", err)
		}
		if recoveredResult != (delivery.SchedulerBatchResult{Claimed: 2, Queued: 2}) {
			t.Fatalf("recovery result = %#v, want first and third messages queued", recoveredResult)
		}
		assertPersistedMessageState(t, ctx, messageRepository, first.Message.ID(), message.StatusQueued, 1)
		assertPersistedMessageState(t, ctx, messageRepository, third.Message.ID(), message.StatusQueued, 1)
		assertOutboxCounts(t, ctx, pool, first.Message.ID(), 4, 1)
		assertOutboxCounts(t, ctx, pool, second.Message.ID(), 4, 1)
		assertOutboxCounts(t, ctx, pool, third.Message.ID(), 4, 1)
	})
}

type scheduledRecordParams struct {
	messageID      string
	tenantID       string
	idempotencyKey string
	fingerprint    [32]byte
	acceptedAt     time.Time
	scheduledAt    time.Time
}

func createScheduledRecord(
	t *testing.T,
	ctx context.Context,
	store *delivery.ReliableMessageStore,
	params scheduledRecordParams,
) ports.MessageRecord {
	t.Helper()
	scheduledAt := params.scheduledAt
	record := newRepositoryRecord(t, recordParams{
		messageID:      params.messageID,
		tenantID:       params.tenantID,
		idempotencyKey: params.idempotencyKey,
		fingerprint:    params.fingerprint,
		now:            params.acceptedAt,
		scheduledAt:    &scheduledAt,
	})
	if _, err := store.Create(ctx, record); err != nil {
		t.Fatalf("create scheduled record %s: %v", params.messageID, err)
	}
	return record
}

func createDueRetryRecord(
	t *testing.T,
	ctx context.Context,
	store *delivery.ReliableMessageStore,
	tenantID string,
	messageID string,
	runAt time.Time,
) ports.MessageRecord {
	t.Helper()
	acceptedAt := runAt.Add(-5 * time.Minute)
	record := newRepositoryRecord(t, recordParams{
		messageID:      messageID,
		tenantID:       tenantID,
		idempotencyKey: "scheduler-retry",
		fingerprint:    fingerprint(0xa4),
		now:            acceptedAt,
	})
	if _, err := store.Create(ctx, record); err != nil {
		t.Fatalf("create retry baseline: %v", err)
	}
	if err := record.Message.StartSending(1, acceptedAt.Add(time.Second)); err != nil {
		t.Fatalf("start retry baseline: %v", err)
	}
	failure := message.Failure{
		Category:  message.FailureRateLimited,
		Code:      "PROVIDER_RATE_LIMITED",
		Retryable: true,
	}
	if err := record.Message.ScheduleRetry(
		failure,
		runAt.Add(-time.Minute),
		acceptedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("schedule retry baseline: %v", err)
	}
	if _, err := store.Save(ctx, record); err != nil {
		t.Fatalf("save retry baseline: %v", err)
	}
	return record
}

func mustDueMessageScheduler(
	t *testing.T,
	transactor ports.Transactor,
	batchSize uint32,
) *delivery.DueMessageScheduler {
	t.Helper()
	scheduler, err := delivery.NewDueMessageScheduler(transactor, batchSize)
	if err != nil {
		t.Fatalf("new due message Scheduler: %v", err)
	}
	return scheduler
}

func assertPersistedMessageState(
	t *testing.T,
	ctx context.Context,
	repository *postgresstore.MessageRepository,
	messageID string,
	wantStatus message.Status,
	wantVersion uint64,
) {
	t.Helper()
	record, err := repository.GetByID(ctx, messageID)
	if err != nil {
		t.Fatalf("load message %s: %v", messageID, err)
	}
	if record.Message.Status() != wantStatus || record.Message.Version() != wantVersion {
		t.Fatalf(
			"message %s state = %s/v%d, want %s/v%d",
			messageID,
			record.Message.Status(),
			record.Message.Version(),
			wantStatus,
			wantVersion,
		)
	}
}

func assertOutboxCounts(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	messageID string,
	wantTotal int,
	wantDispatch int,
) {
	t.Helper()
	var total, dispatch int
	if err := pool.QueryRow(ctx, `
		SELECT
			count(*),
			count(*) FILTER (WHERE event_type = 'MESSAGE_DISPATCH_REQUESTED')
		FROM outbox_events
		WHERE aggregate_id = $1
	`, messageID).Scan(&total, &dispatch); err != nil {
		t.Fatalf("count Outbox for %s: %v", messageID, err)
	}
	if total != wantTotal || dispatch != wantDispatch {
		t.Fatalf(
			"Outbox counts for %s = total:%d dispatch:%d, want total:%d dispatch:%d",
			messageID,
			total,
			dispatch,
			wantTotal,
			wantDispatch,
		)
	}
}
