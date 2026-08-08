//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/delivery"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	fakepublisher "github.com/Zhiruosama/Email-Service/internal/publisher/fake"
	postgresstore "github.com/Zhiruosama/Email-Service/internal/storage/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestOutboxRelaySystem(t *testing.T) {
	ctx := context.Background()
	pool, _ := setupMessageRepository(t)
	transactor := postgresstore.NewTransactionManager(pool)

	t.Run("claims only available events and fences stale owners", func(t *testing.T) {
		available := relayOutboxEvent(
			"90000000-0000-4000-8000-000000000001",
			"91000000-0000-4000-8000-000000000001",
			"RELAY_AVAILABLE",
		)
		future := relayOutboxEvent(
			"90000000-0000-4000-8000-000000000002",
			"91000000-0000-4000-8000-000000000002",
			"RELAY_FUTURE",
		)
		leased := relayOutboxEvent(
			"90000000-0000-4000-8000-000000000003",
			"91000000-0000-4000-8000-000000000003",
			"RELAY_LEASED",
		)
		appendRelayOutboxEvents(t, ctx, transactor, available, future, leased)
		if _, err := pool.Exec(ctx, `
			UPDATE outbox_events
			SET available_at = transaction_timestamp() + INTERVAL '1 hour'
			WHERE id = $1
		`, future.ID); err != nil {
			t.Fatalf("prepare future Outbox event: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			UPDATE outbox_events
			SET lease_owner = 'existing/lease',
				lease_until = transaction_timestamp() + INTERVAL '1 hour'
			WHERE id = $1
		`, leased.ID); err != nil {
			t.Fatalf("prepare leased Outbox event: %v", err)
		}

		first := claimRelayOutbox(t, ctx, transactor, "relay-a/claim", 10)
		if len(first.Events) != 1 || first.Events[0].Event.ID != available.ID {
			t.Fatalf("first claim = %#v, want available event", first)
		}
		if first.EvaluatedAt.IsZero() || first.Events[0].AttemptNumber != 1 {
			t.Fatalf("first claim metadata = %#v", first)
		}
		if empty := claimRelayOutbox(t, ctx, transactor, "relay-b/empty", 10); len(empty.Events) != 0 {
			t.Fatalf("unavailable events were claimed: %#v", empty)
		}

		firstLease := leaseReference(first.Events[0])
		if err := transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
			return unit.OutboxDeliveries().Reschedule(ctx, ports.RescheduleOutboxCommand{
				Lease: firstLease, Delay: 0, ErrorCode: "BROKER_UNAVAILABLE",
			})
		}); err != nil {
			t.Fatalf("reschedule first claim: %v", err)
		}
		second := claimRelayOutbox(t, ctx, transactor, "relay-b/claim", 10)
		if len(second.Events) != 1 || second.Events[0].Event.ID != available.ID ||
			second.Events[0].AttemptNumber != 2 {
			t.Fatalf("second claim = %#v, want available attempt 2", second)
		}

		err := transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
			return unit.OutboxDeliveries().MarkPublished(ctx, firstLease)
		})
		if !errors.Is(err, ports.ErrOutboxLeaseLost) {
			t.Fatalf("stale owner error = %v, want ErrOutboxLeaseLost", err)
		}
		if err := transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
			return unit.OutboxDeliveries().MarkPublished(ctx, leaseReference(second.Events[0]))
		}); err != nil {
			t.Fatalf("publish current owner: %v", err)
		}
		assertRelayOutboxState(t, ctx, pool, available.ID, "PUBLISHED", 2, "", "", true)

		if _, err := pool.Exec(ctx, `
			UPDATE outbox_events
			SET lease_until = transaction_timestamp() - INTERVAL '1 second'
			WHERE id = $1
		`, leased.ID); err != nil {
			t.Fatalf("expire existing lease: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			UPDATE outbox_events
			SET available_at = transaction_timestamp() - INTERVAL '1 second'
			WHERE id = $1
		`, future.ID); err != nil {
			t.Fatalf("make future event available: %v", err)
		}
		recovered := claimRelayOutbox(t, ctx, transactor, "relay-c/recovery", 10)
		if len(recovered.Events) < 2 {
			additional := claimRelayOutbox(t, ctx, transactor, "relay-d/recovery", 10)
			recovered.Events = append(recovered.Events, additional.Events...)
		}
		if len(recovered.Events) != 2 {
			t.Fatalf("recovery claims = %#v, want two events", recovered.Events)
		}
		recoveredIDs := map[string]bool{}
		for _, event := range recovered.Events {
			recoveredIDs[event.Event.ID] = true
			if err := transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
				return unit.OutboxDeliveries().MarkPublished(ctx, leaseReference(event))
			}); err != nil {
				t.Fatalf("publish recovered event %s: %v", event.Event.ID, err)
			}
		}
		if !recoveredIDs[leased.ID] || !recoveredIDs[future.ID] {
			t.Fatalf("recovered event IDs = %#v", recoveredIDs)
		}
		assertRelayOutboxState(t, ctx, pool, leased.ID, "PUBLISHED", 1, "", "", true)
		assertRelayOutboxState(t, ctx, pool, future.ID, "PUBLISHED", 1, "", "", true)
	})

	t.Run("uses skip locked and batch limits", func(t *testing.T) {
		first := relayOutboxEvent(
			"92000000-0000-4000-8000-000000000001",
			"93000000-0000-4000-8000-000000000001",
			"RELAY_LOCKED_FIRST",
		)
		second := relayOutboxEvent(
			"92000000-0000-4000-8000-000000000002",
			"93000000-0000-4000-8000-000000000002",
			"RELAY_SECOND",
		)
		third := relayOutboxEvent(
			"92000000-0000-4000-8000-000000000003",
			"93000000-0000-4000-8000-000000000003",
			"RELAY_THIRD",
		)
		appendRelayOutboxEvents(t, ctx, transactor, first, second, third)

		blockingTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin blocking Relay transaction: %v", err)
		}
		blocked, err := postgresstore.NewOutboxDeliveryRepository(blockingTx).ClaimPending(
			ctx,
			ports.OutboxClaimQuery{
				LeaseToken: "relay-blocking/claim", LeaseDuration: time.Minute, Limit: 1,
			},
		)
		if err != nil {
			_ = blockingTx.Rollback(ctx)
			t.Fatalf("claim blocking Outbox event: %v", err)
		}
		if len(blocked.Events) != 1 || blocked.Events[0].Event.ID != first.ID {
			_ = blockingTx.Rollback(ctx)
			t.Fatalf("blocking claim = %#v, want first", blocked)
		}

		skipCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		skipped := claimRelayOutbox(t, skipCtx, transactor, "relay-skip/claim", 1)
		if len(skipped.Events) != 1 || skipped.Events[0].Event.ID != second.ID {
			_ = blockingTx.Rollback(ctx)
			t.Fatalf("skip-locked claim = %#v, want second", skipped)
		}
		if err := transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
			return unit.OutboxDeliveries().MarkPublished(ctx, leaseReference(skipped.Events[0]))
		}); err != nil {
			_ = blockingTx.Rollback(ctx)
			t.Fatalf("publish skipped claim: %v", err)
		}
		if err := blockingTx.Rollback(ctx); err != nil {
			t.Fatalf("rollback blocking Relay: %v", err)
		}

		recovered := claimRelayOutbox(t, ctx, transactor, "relay-after-rollback/claim", 10)
		if len(recovered.Events) != 2 {
			t.Fatalf("post-rollback claim = %#v, want first and third", recovered)
		}
		for _, event := range recovered.Events {
			if err := transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
				return unit.OutboxDeliveries().MarkPublished(ctx, leaseReference(event))
			}); err != nil {
				t.Fatalf("publish post-rollback event: %v", err)
			}
		}
	})

	t.Run("publishes outside the claim transaction and handles outcomes", func(t *testing.T) {
		events := []ports.OutboxEvent{
			relayOutboxEvent("94000000-0000-4000-8000-000000000001", "95000000-0000-4000-8000-000000000001", "RELAY_SUCCESS"),
			relayOutboxEvent("94000000-0000-4000-8000-000000000002", "95000000-0000-4000-8000-000000000002", "RELAY_RETRY"),
			relayOutboxEvent("94000000-0000-4000-8000-000000000003", "95000000-0000-4000-8000-000000000003", "RELAY_PERMANENT"),
			relayOutboxEvent("94000000-0000-4000-8000-000000000004", "95000000-0000-4000-8000-000000000004", "RELAY_TIMEOUT"),
		}
		appendRelayOutboxEvents(t, ctx, transactor, events...)

		publisher := fakepublisher.New(func(publishCtx context.Context, publication ports.OutboxPublication) error {
			var visible bool
			if err := pool.QueryRow(publishCtx, `
				SELECT lease_owner IS NOT NULL AND lease_until > transaction_timestamp()
				FROM outbox_events
				WHERE id = $1
			`, publication.Event.ID).Scan(&visible); err != nil {
				return ports.NewOutboxPublishError("LEASE_VISIBILITY_CHECK_FAILED", false, err)
			}
			if !visible {
				return ports.NewOutboxPublishError("LEASE_NOT_COMMITTED", false, nil)
			}
			if publication.AttemptNumber > 1 {
				return nil
			}
			switch publication.Event.EventType {
			case "RELAY_SUCCESS":
				return nil
			case "RELAY_RETRY":
				return ports.NewOutboxPublishError("BROKER_UNAVAILABLE", true, nil)
			case "RELAY_PERMANENT":
				return ports.NewOutboxPublishError("UNROUTABLE", false, nil)
			case "RELAY_TIMEOUT":
				<-publishCtx.Done()
				return publishCtx.Err()
			default:
				return fmt.Errorf("unexpected event type %s", publication.Event.EventType)
			}
		})
		relay := mustOutboxRelay(t, transactor, publisher, 4, 3, 50*time.Millisecond)
		result, err := relay.RunOnce(ctx)
		if err != nil {
			t.Fatalf("run first Relay batch: %v", err)
		}
		if result != (delivery.OutboxRelayResult{
			Claimed: 4, Published: 1, Retried: 2, DeadLettered: 1,
		}) {
			t.Fatalf("first Relay result = %#v", result)
		}
		assertRelayOutboxState(t, ctx, pool, events[0].ID, "PUBLISHED", 1, "", "", true)
		assertRelayOutboxState(t, ctx, pool, events[1].ID, "PENDING", 1, "", "BROKER_UNAVAILABLE", false)
		assertRelayOutboxState(t, ctx, pool, events[2].ID, "DEAD_LETTERED", 1, "", "UNROUTABLE", false)
		assertRelayOutboxState(t, ctx, pool, events[3].ID, "PENDING", 1, "", "PUBLISH_TIMEOUT", false)

		secondResult, err := relay.RunOnce(ctx)
		if err != nil {
			t.Fatalf("run second Relay batch: %v", err)
		}
		if secondResult != (delivery.OutboxRelayResult{Claimed: 2, Published: 2}) {
			t.Fatalf("second Relay result = %#v", secondResult)
		}
		assertRelayOutboxState(t, ctx, pool, events[1].ID, "PUBLISHED", 2, "", "", true)
		assertRelayOutboxState(t, ctx, pool, events[3].ID, "PUBLISHED", 2, "", "", true)
	})

	t.Run("dead letters retryable failures at the attempt limit", func(t *testing.T) {
		event := relayOutboxEvent(
			"96000000-0000-4000-8000-000000000001",
			"97000000-0000-4000-8000-000000000001",
			"RELAY_EXHAUSTED",
		)
		appendRelayOutboxEvents(t, ctx, transactor, event)
		publisher := fakepublisher.New(func(context.Context, ports.OutboxPublication) error {
			return ports.NewOutboxPublishError("BROKER_UNAVAILABLE", true, nil)
		})
		relay := mustOutboxRelay(t, transactor, publisher, 1, 2, time.Second)

		first, err := relay.RunOnce(ctx)
		if err != nil || first != (delivery.OutboxRelayResult{Claimed: 1, Retried: 1}) {
			t.Fatalf("first exhausted Relay run = (%#v, %v)", first, err)
		}
		second, err := relay.RunOnce(ctx)
		if err != nil || second != (delivery.OutboxRelayResult{Claimed: 1, DeadLettered: 1}) {
			t.Fatalf("second exhausted Relay run = (%#v, %v)", second, err)
		}
		assertRelayOutboxState(t, ctx, pool, event.ID, "DEAD_LETTERED", 2, "", "BROKER_UNAVAILABLE", false)
	})

	t.Run("repeats publish after a confirmed result was not recorded", func(t *testing.T) {
		event := relayOutboxEvent(
			"98000000-0000-4000-8000-000000000001",
			"99000000-0000-4000-8000-000000000001",
			"RELAY_CONFIRM_LOST",
		)
		appendRelayOutboxEvents(t, ctx, transactor, event)
		crashedClaim := claimRelayOutbox(t, ctx, transactor, "relay-crashed/claim", 1)
		if len(crashedClaim.Events) != 1 {
			t.Fatalf("crashed claim = %#v", crashedClaim)
		}

		var attemptsMu sync.Mutex
		attempts := 0
		publisher := fakepublisher.New(func(context.Context, ports.OutboxPublication) error {
			attemptsMu.Lock()
			attempts++
			attemptsMu.Unlock()
			return nil
		})
		if err := publisher.Publish(ctx, ports.OutboxPublication{
			Event: event, AttemptNumber: crashedClaim.Events[0].AttemptNumber,
		}); err != nil {
			t.Fatalf("simulate confirmed publish: %v", err)
		}
		// Simulate a process crash before MarkPublished and the later expiry of
		// its lease. attempt_count remains zero because no outcome was recorded.
		if _, err := pool.Exec(ctx, `
			UPDATE outbox_events
			SET lease_until = transaction_timestamp() - INTERVAL '1 second'
			WHERE id = $1
		`, event.ID); err != nil {
			t.Fatalf("expire crashed Relay lease: %v", err)
		}

		relay := mustOutboxRelay(t, transactor, publisher, 1, 3, time.Second)
		result, err := relay.RunOnce(ctx)
		if err != nil || result != (delivery.OutboxRelayResult{Claimed: 1, Published: 1}) {
			t.Fatalf("recover confirmed-but-unrecorded event = (%#v, %v)", result, err)
		}
		attemptsMu.Lock()
		gotAttempts := attempts
		attemptsMu.Unlock()
		if gotAttempts != 2 {
			t.Fatalf("physical publish attempts = %d, want 2", gotAttempts)
		}
		assertRelayOutboxState(t, ctx, pool, event.ID, "PUBLISHED", 1, "", "", true)
	})
}

func relayOutboxEvent(id, aggregateID, eventType string) ports.OutboxEvent {
	return testOutboxEvent(id, aggregateID, eventType, 1, `{"schema_version":1}`)
}

func appendRelayOutboxEvents(
	t *testing.T,
	ctx context.Context,
	transactor ports.Transactor,
	events ...ports.OutboxEvent,
) {
	t.Helper()
	if err := transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
		return unit.Outbox().Append(ctx, events)
	}); err != nil {
		t.Fatalf("append Relay Outbox events: %v", err)
	}
}

func claimRelayOutbox(
	t *testing.T,
	ctx context.Context,
	transactor ports.Transactor,
	leaseToken string,
	limit uint32,
) ports.OutboxClaimBatch {
	t.Helper()
	var batch ports.OutboxClaimBatch
	if err := transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
		claimed, err := unit.OutboxDeliveries().ClaimPending(ctx, ports.OutboxClaimQuery{
			LeaseToken: leaseToken, LeaseDuration: time.Minute, Limit: limit,
		})
		batch = claimed
		return err
	}); err != nil {
		t.Fatalf("claim Relay Outbox events: %v", err)
	}
	return batch
}

func leaseReference(event ports.LeasedOutboxEvent) ports.OutboxLeaseReference {
	return ports.OutboxLeaseReference{
		EventID: event.Event.ID, LeaseToken: event.LeaseToken, AttemptNumber: event.AttemptNumber,
	}
}

func mustOutboxRelay(
	t *testing.T,
	transactor ports.Transactor,
	publisher ports.OutboxPublisher,
	batchSize uint32,
	maxAttempts uint32,
	publishTimeout time.Duration,
) *delivery.OutboxRelay {
	t.Helper()
	relay, err := delivery.NewOutboxRelay(
		transactor,
		publisher,
		fixedIntegrationRetryPolicy(0),
		delivery.OutboxRelayConfig{
			InstanceID:         "integration-relay",
			BatchSize:          batchSize,
			PublishConcurrency: batchSize,
			LeaseDuration:      time.Minute,
			PublishTimeout:     publishTimeout,
			MaxAttempts:        maxAttempts,
		},
	)
	if err != nil {
		t.Fatalf("new Outbox Relay: %v", err)
	}
	return relay
}

type fixedIntegrationRetryPolicy time.Duration

func (policy fixedIntegrationRetryPolicy) NextDelay(uint32) time.Duration {
	return time.Duration(policy)
}

type relayOutboxState struct {
	status        string
	attemptCount  int
	leaseOwner    string
	lastErrorCode string
	published     bool
}

func assertRelayOutboxState(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	eventID string,
	wantStatus string,
	wantAttemptCount int,
	wantLeaseOwner string,
	wantErrorCode string,
	wantPublished bool,
) {
	t.Helper()
	var got relayOutboxState
	if err := pool.QueryRow(ctx, `
		SELECT
			status,
			attempt_count,
			COALESCE(lease_owner, ''),
			COALESCE(last_error_code, ''),
			published_at IS NOT NULL
		FROM outbox_events
		WHERE id = $1
	`, eventID).Scan(
		&got.status,
		&got.attemptCount,
		&got.leaseOwner,
		&got.lastErrorCode,
		&got.published,
	); err != nil {
		t.Fatalf("read Relay Outbox state %s: %v", eventID, err)
	}
	want := relayOutboxState{
		status:        wantStatus,
		attemptCount:  wantAttemptCount,
		leaseOwner:    wantLeaseOwner,
		lastErrorCode: wantErrorCode,
		published:     wantPublished,
	}
	if got != want {
		t.Fatalf("Relay Outbox state %s = %#v, want %#v", eventID, got, want)
	}
}
