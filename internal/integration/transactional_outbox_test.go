//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/delivery"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	postgresstore "github.com/Zhiruosama/Email-Service/internal/storage/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestTransactionalOutbox(t *testing.T) {
	ctx := context.Background()
	pool, messageRepository := setupMessageRepository(t)
	transactor := postgresstore.NewTransactionManager(pool)
	store := delivery.NewReliableMessageStore(transactor)

	t.Run("creates message and all domain events atomically", func(t *testing.T) {
		const tenantID = "70000000-0000-4000-8000-000000000001"
		insertRepositoryTenant(t, ctx, pool, tenantID, "outbox-create")
		now := time.Date(2026, 8, 9, 9, 0, 0, 0, time.UTC)
		record := newRepositoryRecord(t, recordParams{
			messageID:      "71000000-0000-4000-8000-000000000001",
			tenantID:       tenantID,
			idempotencyKey: "immediate-with-outbox",
			fingerprint:    fingerprint(0x91),
			now:            now,
		})
		if len(record.Message.PendingEvents()) != 3 {
			t.Fatalf("immediate pending events = %d, want 3", len(record.Message.PendingEvents()))
		}

		result, err := store.Create(ctx, record)
		if err != nil {
			t.Fatalf("reliable create: %v", err)
		}
		if result.Disposition != ports.CreateDispositionCreated {
			t.Fatalf("create disposition = %q, want CREATED", result.Disposition)
		}
		if len(record.Message.PendingEvents()) != 0 {
			t.Fatal("committed create did not clear pending events")
		}
		assertMessageOutboxEvents(t, ctx, pool, record.Message.ID(), tenantID, map[string][2]int64{
			"MESSAGE_ACCEPTED":           {1, 0},
			"MESSAGE_STATUS_CHANGED":     {2, 1},
			"MESSAGE_DISPATCH_REQUESTED": {2, 1},
		})
		if countDeliveryEvents(t, ctx, pool, record.Message.ID()) != 2 {
			t.Fatal("immediate message does not have two lifecycle journal events")
		}

		duplicate := newRepositoryRecord(t, recordParams{
			messageID:      "71000000-0000-4000-8000-000000000002",
			tenantID:       tenantID,
			idempotencyKey: record.IdempotencyKey,
			fingerprint:    record.PayloadFingerprint,
			now:            now,
		})
		duplicateResult, err := store.Create(ctx, duplicate)
		if err != nil {
			t.Fatalf("duplicate reliable create: %v", err)
		}
		if duplicateResult.Disposition != ports.CreateDispositionDuplicate {
			t.Fatalf("duplicate disposition = %q, want DUPLICATE", duplicateResult.Disposition)
		}
		if countOutboxEvents(t, ctx, pool, record.Message.ID()) != 3 {
			t.Fatal("duplicate create generated additional outbox events")
		}
		if countDeliveryEvents(t, ctx, pool, record.Message.ID()) != 2 {
			t.Fatal("duplicate create generated additional journal events")
		}

		scheduledAt := now.Add(5 * time.Minute)
		scheduled := newRepositoryRecord(t, recordParams{
			messageID:      "71000000-0000-4000-8000-000000000003",
			tenantID:       tenantID,
			idempotencyKey: "scheduled-with-outbox",
			fingerprint:    fingerprint(0x92),
			now:            now,
			scheduledAt:    &scheduledAt,
		})
		if _, err := store.Create(ctx, scheduled); err != nil {
			t.Fatalf("create scheduled message: %v", err)
		}
		assertMessageOutboxEvents(t, ctx, pool, scheduled.Message.ID(), tenantID, map[string][2]int64{
			"MESSAGE_ACCEPTED":       {1, 0},
			"MESSAGE_STATUS_CHANGED": {2, 0},
		})
		if countDeliveryEvents(t, ctx, pool, scheduled.Message.ID()) != 2 {
			t.Fatal("scheduled message does not have two initial journal events")
		}
	})

	t.Run("rolls back message when outbox insert fails", func(t *testing.T) {
		const tenantID = "70000000-0000-4000-8000-000000000002"
		insertRepositoryTenant(t, ctx, pool, tenantID, "outbox-create-rollback")
		installRejectingOutboxTrigger(t, ctx, pool)
		now := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
		record := newRepositoryRecord(t, recordParams{
			messageID:      "72000000-0000-4000-8000-000000000001",
			tenantID:       tenantID,
			idempotencyKey: "forced-outbox-failure",
			fingerprint:    fingerprint(0x93),
			now:            now,
		})

		if _, err := store.Create(ctx, record); !errors.Is(err, ports.ErrOutboxRepository) {
			t.Fatalf("reliable create error = %v, want ErrOutboxRepository", err)
		}
		if _, err := messageRepository.GetByID(ctx, record.Message.ID()); !errors.Is(err, ports.ErrMessageNotFound) {
			t.Fatalf("message survived outbox rollback: %v", err)
		}
		if countOutboxEvents(t, ctx, pool, record.Message.ID()) != 0 {
			t.Fatal("outbox row survived failed transaction")
		}
		if countDeliveryEvents(t, ctx, pool, record.Message.ID()) != 0 {
			t.Fatal("journal row survived failed outbox transaction")
		}
		if len(record.Message.PendingEvents()) != 3 {
			t.Fatal("failed transaction cleared pending events")
		}
	})

	t.Run("saves state and events atomically with optimistic locking", func(t *testing.T) {
		const tenantID = "70000000-0000-4000-8000-000000000003"
		insertRepositoryTenant(t, ctx, pool, tenantID, "outbox-save")
		now := time.Date(2026, 8, 9, 11, 0, 0, 0, time.UTC)
		scheduledAt := now.Add(time.Minute)
		record := newRepositoryRecord(t, recordParams{
			messageID:      "73000000-0000-4000-8000-000000000001",
			tenantID:       tenantID,
			idempotencyKey: "save-with-outbox",
			fingerprint:    fingerprint(0x94),
			now:            now,
			scheduledAt:    &scheduledAt,
		})
		if _, err := store.Create(ctx, record); err != nil {
			t.Fatalf("create scheduled baseline: %v", err)
		}

		queueCandidate, err := messageRepository.GetByID(ctx, record.Message.ID())
		if err != nil {
			t.Fatalf("load queue candidate: %v", err)
		}
		cancelCandidate, err := messageRepository.GetByID(ctx, record.Message.ID())
		if err != nil {
			t.Fatalf("load cancel candidate: %v", err)
		}
		if err := queueCandidate.Message.Queue(scheduledAt); err != nil {
			t.Fatalf("queue message: %v", err)
		}
		if _, err := cancelCandidate.Message.Cancel("CALLER_CANCELED", now.Add(30*time.Second)); err != nil {
			t.Fatalf("cancel stale candidate: %v", err)
		}

		version, err := store.Save(ctx, queueCandidate)
		if err != nil {
			t.Fatalf("reliable save: %v", err)
		}
		if version != 1 || len(queueCandidate.Message.PendingEvents()) != 0 {
			t.Fatalf("save version/events = %d/%d, want 1/0", version, len(queueCandidate.Message.PendingEvents()))
		}
		if countOutboxEvents(t, ctx, pool, record.Message.ID()) != 4 {
			t.Fatal("queued message does not have four total outbox events")
		}
		if countDeliveryEvents(t, ctx, pool, record.Message.ID()) != 3 {
			t.Fatal("queued message does not have three lifecycle journal events")
		}

		if _, err := store.Save(ctx, cancelCandidate); !errors.Is(err, ports.ErrConcurrentUpdate) {
			t.Fatalf("stale save error = %v, want ErrConcurrentUpdate", err)
		}
		if countOutboxEvents(t, ctx, pool, record.Message.ID()) != 4 {
			t.Fatal("stale save inserted an outbox event")
		}
		if countDeliveryEvents(t, ctx, pool, record.Message.ID()) != 3 {
			t.Fatal("stale save inserted a journal event")
		}
		if len(cancelCandidate.Message.PendingEvents()) == 0 {
			t.Fatal("stale save cleared pending events")
		}

		current, err := messageRepository.GetByID(ctx, record.Message.ID())
		if err != nil {
			t.Fatalf("load current message: %v", err)
		}
		if _, err := current.Message.Cancel("ROLLBACK_TEST", now.Add(2*time.Minute)); err != nil {
			t.Fatalf("cancel current message: %v", err)
		}
		installRejectingOutboxTrigger(t, ctx, pool)
		if _, err := store.Save(ctx, current); !errors.Is(err, ports.ErrOutboxRepository) {
			t.Fatalf("forced save error = %v, want ErrOutboxRepository", err)
		}
		persisted, err := messageRepository.GetByID(ctx, record.Message.ID())
		if err != nil {
			t.Fatalf("load after rollback: %v", err)
		}
		if persisted.Message.Status() != message.StatusQueued || persisted.Message.Version() != 1 {
			t.Fatalf("rolled-back state = %s/v%d, want QUEUED/v1", persisted.Message.Status(), persisted.Message.Version())
		}
		if countOutboxEvents(t, ctx, pool, record.Message.ID()) != 4 {
			t.Fatal("forced save failure changed outbox count")
		}
		if countDeliveryEvents(t, ctx, pool, record.Message.ID()) != 3 {
			t.Fatal("forced save failure changed journal count")
		}
		if len(current.Message.PendingEvents()) == 0 {
			t.Fatal("forced save failure cleared pending events")
		}
	})

	t.Run("deduplicates identical outbox payload and rejects divergence", func(t *testing.T) {
		aggregateID := "74000000-0000-4000-8000-000000000001"
		first := testOutboxEvent(
			"75000000-0000-4000-8000-000000000001",
			aggregateID,
			"TEST_EVENT",
			1,
			`{"a":1,"b":2}`,
		)
		if err := transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
			return unit.Outbox().Append(ctx, []ports.OutboxEvent{first})
		}); err != nil {
			t.Fatalf("append first outbox event: %v", err)
		}

		duplicate := first
		duplicate.ID = "75000000-0000-4000-8000-000000000002"
		duplicate.Payload = []byte(`{"b":2, "a":1}`)
		if err := transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
			return unit.Outbox().Append(ctx, []ports.OutboxEvent{duplicate})
		}); err != nil {
			t.Fatalf("append semantically identical event: %v", err)
		}
		if countOutboxEvents(t, ctx, pool, aggregateID) != 1 {
			t.Fatal("identical outbox payload was duplicated")
		}

		divergent := duplicate
		divergent.ID = "75000000-0000-4000-8000-000000000003"
		divergent.Payload = []byte(`{"a":2,"b":2}`)
		err := transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
			return unit.Outbox().Append(ctx, []ports.OutboxEvent{divergent})
		})
		if !errors.Is(err, ports.ErrOutboxConflict) {
			t.Fatalf("divergent payload error = %v, want ErrOutboxConflict", err)
		}
		if countOutboxEvents(t, ctx, pool, aggregateID) != 1 {
			t.Fatal("divergent outbox payload changed persisted rows")
		}

		idConflict := testOutboxEvent(first.ID, aggregateID, "ANOTHER_EVENT", 2, `{}`)
		err = transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
			return unit.Outbox().Append(ctx, []ports.OutboxEvent{idConflict})
		})
		if !errors.Is(err, ports.ErrOutboxIDConflict) {
			t.Fatalf("outbox id conflict error = %v, want ErrOutboxIDConflict", err)
		}
	})

	t.Run("rolls back callback error and panic", func(t *testing.T) {
		callbackAggregate := "76000000-0000-4000-8000-000000000001"
		callbackEvent := testOutboxEvent(
			"77000000-0000-4000-8000-000000000001",
			callbackAggregate,
			"CALLBACK_ERROR",
			1,
			`{}`,
		)
		sentinel := errors.New("forced callback failure")
		err := transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
			if err := unit.Outbox().Append(ctx, []ports.OutboxEvent{callbackEvent}); err != nil {
				return err
			}
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("callback error = %v, want sentinel", err)
		}
		if countOutboxEvents(t, ctx, pool, callbackAggregate) != 0 {
			t.Fatal("callback error did not roll back outbox event")
		}

		panicAggregate := "76000000-0000-4000-8000-000000000002"
		panicEvent := testOutboxEvent(
			"77000000-0000-4000-8000-000000000002",
			panicAggregate,
			"CALLBACK_PANIC",
			1,
			`{}`,
		)
		recovered := capturePanic(func() {
			_ = transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
				if err := unit.Outbox().Append(ctx, []ports.OutboxEvent{panicEvent}); err != nil {
					return err
				}
				panic("forced callback panic")
			})
		})
		if recovered != "forced callback panic" {
			t.Fatalf("recovered panic = %#v, want original panic", recovered)
		}
		if countOutboxEvents(t, ctx, pool, panicAggregate) != 0 {
			t.Fatal("callback panic did not roll back outbox event")
		}
	})
}

func assertMessageOutboxEvents(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	messageID string,
	tenantID string,
	want map[string][2]int64,
) {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT event_type, aggregate_sequence, dispatch_generation, payload
		FROM outbox_events
		WHERE aggregate_id = $1
	`, messageID)
	if err != nil {
		t.Fatalf("query outbox events: %v", err)
	}
	defer rows.Close()
	got := make(map[string][2]int64, len(want))
	for rows.Next() {
		var eventType string
		var sequence, generation int64
		var payload []byte
		if err := rows.Scan(&eventType, &sequence, &generation, &payload); err != nil {
			t.Fatalf("scan outbox event: %v", err)
		}
		if _, duplicate := got[eventType]; duplicate {
			t.Fatalf("duplicate event type %q for message %s", eventType, messageID)
		}
		got[eventType] = [2]int64{sequence, generation}

		var envelope map[string]any
		if err := json.Unmarshal(payload, &envelope); err != nil {
			t.Fatalf("decode outbox payload: %v", err)
		}
		if envelope["schema_version"] != float64(1) || envelope["tenant_id"] != tenantID || envelope["message_id"] != messageID {
			t.Fatalf("unexpected outbox envelope: %s", payload)
		}
		lowerPayload := strings.ToLower(string(payload))
		for _, forbidden := range []string{"@", "recipient_email", "template_variables", "verification_code"} {
			if strings.Contains(lowerPayload, forbidden) {
				t.Fatalf("outbox payload contains forbidden marker %q: %s", forbidden, payload)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate outbox events: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("outbox events = %#v, want %#v", got, want)
	}
	for eventType, expected := range want {
		if got[eventType] != expected {
			t.Fatalf("outbox event %s = %v, want %v", eventType, got[eventType], expected)
		}
	}
}

func countOutboxEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, aggregateID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE aggregate_id = $1", aggregateID).Scan(&count); err != nil {
		t.Fatalf("count outbox events: %v", err)
	}
	return count
}

func countDeliveryEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, messageID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM delivery_events WHERE message_id = $1", messageID).Scan(&count); err != nil {
		t.Fatalf("count delivery events: %v", err)
	}
	return count
}

func installRejectingOutboxTrigger(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION reject_outbox_for_test() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'forced outbox failure';
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER reject_outbox_for_test
		BEFORE INSERT ON outbox_events
		FOR EACH ROW EXECUTE FUNCTION reject_outbox_for_test();
	`); err != nil {
		t.Fatalf("install rejecting outbox trigger: %v", err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `
			DROP TRIGGER IF EXISTS reject_outbox_for_test ON outbox_events;
			DROP FUNCTION IF EXISTS reject_outbox_for_test();
		`); err != nil {
			t.Errorf("remove rejecting outbox trigger: %v", err)
		}
	})
}

func testOutboxEvent(id, aggregateID, eventType string, sequence uint64, payload string) ports.OutboxEvent {
	return ports.OutboxEvent{
		ID:                id,
		AggregateType:     ports.OutboxAggregateMailMessage,
		AggregateID:       aggregateID,
		EventType:         eventType,
		AggregateSequence: sequence,
		Payload:           []byte(payload),
	}
}

func capturePanic(function func()) (recovered any) {
	defer func() { recovered = recover() }()
	function()
	return nil
}
