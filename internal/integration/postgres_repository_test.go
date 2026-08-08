//go:build integration

package integration_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/db/migrations"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	postgresstore "github.com/Zhiruosama/Email-Service/internal/storage/postgres"
	"github.com/Zhiruosama/Email-Service/internal/testkit/postgrescontainer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
)

const (
	repositoryTenantOne = "40000000-0000-4000-8000-000000000001"
	repositoryTenantTwo = "40000000-0000-4000-8000-000000000002"
)

func TestMessageRepositoryRoundTripAndIdempotency(t *testing.T) {
	ctx := context.Background()
	pool, repository := setupMessageRepository(t)
	insertRepositoryTenant(t, ctx, pool, repositoryTenantOne, "repository-one")
	insertRepositoryTenant(t, ctx, pool, repositoryTenantTwo, "repository-two")

	now := time.Date(2026, 8, 8, 13, 0, 0, 0, time.UTC)
	scheduledAt := now.Add(2 * time.Minute)
	record := newRepositoryRecord(t, recordParams{
		messageID:      "50000000-0000-4000-8000-000000000001",
		tenantID:       repositoryTenantOne,
		idempotencyKey: "round-trip",
		fingerprint:    fingerprint(0x11),
		now:            now,
		scheduledAt:    &scheduledAt,
	})
	originalSnapshot := record.Message.Snapshot()
	originalEventCount := len(record.Message.PendingEvents())

	created, err := repository.Create(ctx, record)
	if err != nil {
		t.Fatalf("create message: %v", err)
	}
	if created.Disposition != ports.CreateDispositionCreated {
		t.Fatalf("create disposition = %q, want CREATED", created.Disposition)
	}
	if len(record.Message.PendingEvents()) != originalEventCount {
		t.Fatal("Create cleared pending domain events")
	}

	byID, err := repository.GetByID(ctx, record.Message.ID())
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}
	assertRecordMatches(t, byID, record, originalSnapshot)
	if len(byID.Message.PendingEvents()) != 0 {
		t.Fatal("restored message unexpectedly contains pending events")
	}

	byKey, err := repository.GetByIdempotencyKey(ctx, repositoryTenantOne, "round-trip")
	if err != nil {
		t.Fatalf("get by idempotency key: %v", err)
	}
	assertRecordMatches(t, byKey, record, originalSnapshot)

	duplicate := newRepositoryRecord(t, recordParams{
		messageID:      "50000000-0000-4000-8000-000000000002",
		tenantID:       repositoryTenantOne,
		idempotencyKey: "round-trip",
		fingerprint:    record.PayloadFingerprint,
		now:            now,
		scheduledAt:    &scheduledAt,
	})
	duplicateResult, err := repository.Create(ctx, duplicate)
	if err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	if duplicateResult.Disposition != ports.CreateDispositionDuplicate {
		t.Fatalf("duplicate disposition = %q, want DUPLICATE", duplicateResult.Disposition)
	}
	if duplicateResult.Record.Message.ID() != record.Message.ID() {
		t.Fatalf("duplicate returned message %q, want %q", duplicateResult.Record.Message.ID(), record.Message.ID())
	}

	conflict := duplicate
	conflict.PayloadFingerprint = fingerprint(0x22)
	if _, err := repository.Create(ctx, conflict); !errors.Is(err, ports.ErrIdempotencyConflict) {
		t.Fatalf("different fingerprint error = %v, want ErrIdempotencyConflict", err)
	}

	otherTenant := newRepositoryRecord(t, recordParams{
		messageID:      "50000000-0000-4000-8000-000000000003",
		tenantID:       repositoryTenantTwo,
		idempotencyKey: "round-trip",
		fingerprint:    fingerprint(0x33),
		now:            now,
		scheduledAt:    &scheduledAt,
	})
	if result, err := repository.Create(ctx, otherTenant); err != nil {
		t.Fatalf("same key in another tenant: %v", err)
	} else if result.Disposition != ports.CreateDispositionCreated {
		t.Fatalf("other tenant disposition = %q, want CREATED", result.Disposition)
	}

	messageIDConflict := newRepositoryRecord(t, recordParams{
		messageID:      record.Message.ID(),
		tenantID:       repositoryTenantTwo,
		idempotencyKey: "different-key",
		fingerprint:    fingerprint(0x44),
		now:            now,
		scheduledAt:    &scheduledAt,
	})
	if _, err := repository.Create(ctx, messageIDConflict); !errors.Is(err, ports.ErrMessageIDConflict) {
		t.Fatalf("message id conflict error = %v, want ErrMessageIDConflict", err)
	}

	if _, err := repository.GetByID(ctx, "50000000-0000-4000-8000-000000000099"); !errors.Is(err, ports.ErrMessageNotFound) {
		t.Fatalf("missing id error = %v, want ErrMessageNotFound", err)
	}
	if _, err := repository.GetByIdempotencyKey(ctx, repositoryTenantOne, "missing"); !errors.Is(err, ports.ErrMessageNotFound) {
		t.Fatalf("missing key error = %v, want ErrMessageNotFound", err)
	}
}

func TestMessageRepositoryRoundTripsRetryFailure(t *testing.T) {
	ctx := context.Background()
	pool, repository := setupMessageRepository(t)
	insertRepositoryTenant(t, ctx, pool, repositoryTenantOne, "repository-retry")

	now := time.Date(2026, 8, 8, 14, 0, 0, 0, time.UTC)
	record := newRepositoryRecord(t, recordParams{
		messageID:      "51000000-0000-4000-8000-000000000001",
		tenantID:       repositoryTenantOne,
		idempotencyKey: "retry-round-trip",
		fingerprint:    fingerprint(0x55),
		now:            now,
	})
	if err := record.Message.StartSending(1, now.Add(time.Second)); err != nil {
		t.Fatalf("start sending: %v", err)
	}
	failure := message.Failure{
		Category:  message.FailureRateLimited,
		Code:      "PROVIDER_RATE_LIMITED",
		Retryable: true,
	}
	if err := record.Message.ScheduleRetry(
		failure,
		now.Add(3*time.Minute),
		now.Add(2*time.Second),
	); err != nil {
		t.Fatalf("schedule retry: %v", err)
	}
	wantSnapshot := record.Message.Snapshot()

	if _, err := repository.Create(ctx, record); err != nil {
		t.Fatalf("create retry message: %v", err)
	}
	loaded, err := repository.GetByID(ctx, record.Message.ID())
	if err != nil {
		t.Fatalf("load retry message: %v", err)
	}
	if !reflect.DeepEqual(loaded.Message.Snapshot(), wantSnapshot) {
		t.Fatalf("retry snapshot mismatch\n got: %#v\nwant: %#v", loaded.Message.Snapshot(), wantSnapshot)
	}
}

func TestMessageRepositoryOptimisticConcurrency(t *testing.T) {
	ctx := context.Background()
	pool, repository := setupMessageRepository(t)
	insertRepositoryTenant(t, ctx, pool, repositoryTenantOne, "repository-concurrency")

	now := time.Date(2026, 8, 8, 15, 0, 0, 0, time.UTC)
	scheduledAt := now.Add(time.Minute)
	record := newRepositoryRecord(t, recordParams{
		messageID:      "52000000-0000-4000-8000-000000000001",
		tenantID:       repositoryTenantOne,
		idempotencyKey: "concurrent-update",
		fingerprint:    fingerprint(0x66),
		now:            now,
		scheduledAt:    &scheduledAt,
	})
	if _, err := repository.Create(ctx, record); err != nil {
		t.Fatalf("create message: %v", err)
	}

	queueCandidate, err := repository.GetByID(ctx, record.Message.ID())
	if err != nil {
		t.Fatalf("load queue candidate: %v", err)
	}
	cancelCandidate, err := repository.GetByID(ctx, record.Message.ID())
	if err != nil {
		t.Fatalf("load cancel candidate: %v", err)
	}
	if err := queueCandidate.Message.Queue(scheduledAt); err != nil {
		t.Fatalf("queue candidate: %v", err)
	}
	if _, err := cancelCandidate.Message.Cancel("CALLER_CANCELED", now.Add(30*time.Second)); err != nil {
		t.Fatalf("cancel candidate: %v", err)
	}

	type saveResult struct {
		name    string
		version uint64
		err     error
	}
	results := make(chan saveResult, 2)
	start := make(chan struct{})
	go func() {
		<-start
		version, err := repository.Save(ctx, queueCandidate.Message)
		results <- saveResult{name: "queue", version: version, err: err}
	}()
	go func() {
		<-start
		version, err := repository.Save(ctx, cancelCandidate.Message)
		results <- saveResult{name: "cancel", version: version, err: err}
	}()
	close(start)

	first := <-results
	second := <-results
	close(results)
	var winner, loser saveResult
	if first.err == nil {
		winner, loser = first, second
	} else {
		winner, loser = second, first
	}
	if winner.err != nil {
		t.Fatalf("both optimistic updates failed: first=%v second=%v", first.err, second.err)
	}
	if winner.version != 1 {
		t.Fatalf("winner version = %d, want 1", winner.version)
	}
	if !errors.Is(loser.err, ports.ErrConcurrentUpdate) {
		t.Fatalf("loser error = %v, want ErrConcurrentUpdate", loser.err)
	}

	persisted, err := repository.GetByID(ctx, record.Message.ID())
	if err != nil {
		t.Fatalf("load winning state: %v", err)
	}
	wantStatus := message.StatusCanceled
	if winner.name == "queue" {
		wantStatus = message.StatusQueued
	}
	if persisted.Message.Status() != wantStatus {
		t.Fatalf("persisted status = %s, want winner status %s", persisted.Message.Status(), wantStatus)
	}
	if persisted.Message.Version() != 1 {
		t.Fatalf("persisted version = %d, want 1", persisted.Message.Version())
	}
	if queueCandidate.Message.Version() != 0 || cancelCandidate.Message.Version() != 0 {
		t.Fatal("Save mutated request-scoped aggregate version")
	}
	if len(queueCandidate.Message.PendingEvents()) == 0 || len(cancelCandidate.Message.PendingEvents()) == 0 {
		t.Fatal("Save cleared pending events on a winner or loser")
	}
}

func TestMessageRepositoryWorksInsideTransaction(t *testing.T) {
	ctx := context.Background()
	pool, repository := setupMessageRepository(t)
	insertRepositoryTenant(t, ctx, pool, repositoryTenantOne, "repository-transaction")

	now := time.Date(2026, 8, 8, 16, 0, 0, 0, time.UTC)
	record := newRepositoryRecord(t, recordParams{
		messageID:      "53000000-0000-4000-8000-000000000001",
		tenantID:       repositoryTenantOne,
		idempotencyKey: "transaction-rollback",
		fingerprint:    fingerprint(0x77),
		now:            now,
	})

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	txRepository := postgresstore.NewMessageRepository(tx)
	if _, err := txRepository.Create(ctx, record); err != nil {
		t.Fatalf("create inside transaction: %v", err)
	}
	if _, err := txRepository.GetByID(ctx, record.Message.ID()); err != nil {
		t.Fatalf("read own transaction write: %v", err)
	}
	if _, err := repository.GetByID(ctx, record.Message.ID()); !errors.Is(err, ports.ErrMessageNotFound) {
		t.Fatalf("uncommitted row visible outside transaction: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("roll back transaction: %v", err)
	}
	if _, err := repository.GetByID(ctx, record.Message.ID()); !errors.Is(err, ports.ErrMessageNotFound) {
		t.Fatalf("rolled-back row still visible: %v", err)
	}

	if _, err := repository.Create(ctx, record); err != nil {
		t.Fatalf("create committed baseline: %v", err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin idempotency transaction: %v", err)
	}
	txRepository = postgresstore.NewMessageRepository(tx)
	duplicate := newRepositoryRecord(t, recordParams{
		messageID:      "53000000-0000-4000-8000-000000000002",
		tenantID:       repositoryTenantOne,
		idempotencyKey: record.IdempotencyKey,
		fingerprint:    record.PayloadFingerprint,
		now:            now,
	})
	result, err := txRepository.Create(ctx, duplicate)
	if err != nil {
		t.Fatalf("idempotent create inside transaction: %v", err)
	}
	if result.Disposition != ports.CreateDispositionDuplicate {
		t.Fatalf("transaction duplicate disposition = %q, want DUPLICATE", result.Disposition)
	}
	var one int
	if err := tx.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("transaction was aborted by idempotency conflict: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("roll back idempotency transaction: %v", err)
	}
}

func TestMessageRepositoryRejectsCorruptSnapshot(t *testing.T) {
	ctx := context.Background()
	pool, repository := setupMessageRepository(t)
	insertRepositoryTenant(t, ctx, pool, repositoryTenantOne, "repository-corruption")

	now := time.Date(2026, 8, 8, 17, 0, 0, 0, time.UTC)
	record := newRepositoryRecord(t, recordParams{
		messageID:      "54000000-0000-4000-8000-000000000001",
		tenantID:       repositoryTenantOne,
		idempotencyKey: "corrupt-snapshot",
		fingerprint:    fingerprint(0x88),
		now:            now,
	})
	if _, err := repository.Create(ctx, record); err != nil {
		t.Fatalf("create message: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin corruption transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "ALTER TABLE mail_messages DROP CONSTRAINT mail_messages_status_valid"); err != nil {
		t.Fatalf("temporarily drop status constraint: %v", err)
	}
	if _, err := tx.Exec(ctx, "UPDATE mail_messages SET status = 'BROKEN' WHERE id = $1", record.Message.ID()); err != nil {
		t.Fatalf("write corrupt status: %v", err)
	}
	txRepository := postgresstore.NewMessageRepository(tx)
	if _, err := txRepository.GetByID(ctx, record.Message.ID()); !errors.Is(err, ports.ErrCorruptMessageRecord) {
		t.Fatalf("corrupt snapshot error = %v, want ErrCorruptMessageRecord", err)
	}
}

func setupMessageRepository(t *testing.T) (*pgxpool.Pool, *postgresstore.MessageRepository) {
	t.Helper()
	instance := postgrescontainer.StartInstance(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	goose.SetBaseFS(migrations.Files)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("set Goose dialect: %v", err)
	}
	if err := goose.UpContext(ctx, instance.SQL, "sql"); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	config, err := pgxpool.ParseConfig(instance.ConnectionString)
	if err != nil {
		t.Fatalf("parse pgx pool config: %v", err)
	}
	config.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping pgx pool: %v", err)
	}
	return pool, postgresstore.NewMessageRepository(pool)
}

func insertRepositoryTenant(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	id string,
	key string,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO tenants (id, key, name, default_locale)
		VALUES ($1, $2, $3, 'zh-CN')
	`, id, key, key+" name"); err != nil {
		t.Fatalf("insert repository tenant: %v", err)
	}
}

type recordParams struct {
	messageID      string
	tenantID       string
	idempotencyKey string
	fingerprint    [32]byte
	now            time.Time
	scheduledAt    *time.Time
}

func newRepositoryRecord(t *testing.T, params recordParams) ports.MessageRecord {
	t.Helper()
	aggregate, err := message.New(message.NewParams{
		ID:               params.messageID,
		Now:              params.now,
		ScheduledAt:      params.scheduledAt,
		DispatchDeadline: params.now.Add(10 * time.Minute),
		MaxAttempts:      3,
	})
	if err != nil {
		t.Fatalf("create domain message: %v", err)
	}
	return ports.MessageRecord{
		TenantID:            params.tenantID,
		IdempotencyKey:      params.idempotencyKey,
		PayloadFingerprint:  params.fingerprint,
		Category:            ports.EmailCategoryCritical,
		Priority:            9,
		DuplicateRiskPolicy: ports.DuplicateRiskAvoidDuplicate,
		Message:             aggregate,
	}
}

func fingerprint(value byte) [32]byte {
	var result [32]byte
	for index := range result {
		result[index] = value
	}
	return result
}

func assertRecordMatches(
	t *testing.T,
	got ports.MessageRecord,
	want ports.MessageRecord,
	wantSnapshot message.Snapshot,
) {
	t.Helper()
	if got.TenantID != want.TenantID ||
		got.IdempotencyKey != want.IdempotencyKey ||
		got.PayloadFingerprint != want.PayloadFingerprint ||
		got.Category != want.Category ||
		got.Priority != want.Priority ||
		got.DuplicateRiskPolicy != want.DuplicateRiskPolicy {
		t.Fatalf("record metadata mismatch\n got: %#v\nwant: %#v", got, want)
	}
	if !reflect.DeepEqual(got.Message.Snapshot(), wantSnapshot) {
		t.Fatalf("message snapshot mismatch\n got: %#v\nwant: %#v", got.Message.Snapshot(), wantSnapshot)
	}
}
