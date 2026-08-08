//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/db/migrations"
	"github.com/Zhiruosama/Email-Service/internal/testkit/postgrescontainer"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
)

const (
	tenantOne  = "10000000-0000-4000-8000-000000000001"
	tenantTwo  = "10000000-0000-4000-8000-000000000002"
	messageOne = "20000000-0000-4000-8000-000000000001"
)

type messageRow struct {
	ID                 string
	TenantID           string
	IdempotencyKey     string
	Fingerprint        []byte
	Status             string
	ScheduledAt        *time.Time
	DispatchDeadline   time.Time
	NextAttemptAt      *time.Time
	DispatchGeneration int64
	AttemptCount       int
	MaxAttempts        int
	LatestSequence     int64
}

func TestDeliveryCoreMigration(t *testing.T) {
	db := postgrescontainer.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	goose.SetBaseFS(migrations.Files)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("set Goose dialect: %v", err)
	}
	if err := goose.UpContext(ctx, db, "sql"); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	t.Run("uses PostgreSQL 18", func(t *testing.T) {
		var version int
		if err := db.QueryRowContext(ctx, "SHOW server_version_num").Scan(&version); err != nil {
			t.Fatalf("query server version: %v", err)
		}
		if version < 180000 || version >= 190000 {
			t.Fatalf("server version = %d, want PostgreSQL 18.x", version)
		}
	})

	t.Run("records migration version", func(t *testing.T) {
		var version int64
		if err := db.QueryRowContext(ctx, `
			SELECT max(version_id)
			FROM goose_db_version
			WHERE is_applied
		`).Scan(&version); err != nil {
			t.Fatalf("query migration version: %v", err)
		}
		if version != 1 {
			t.Fatalf("migration version = %d, want 1", version)
		}
	})

	insertTenant(t, ctx, db, tenantOne, "tenant-one")
	insertTenant(t, ctx, db, tenantTwo, "tenant-two")

	acceptedAt := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	scheduledAt := acceptedAt.Add(time.Minute)
	validMessage := messageRow{
		ID:                 messageOne,
		TenantID:           tenantOne,
		IdempotencyKey:     "verification-001",
		Fingerprint:        bytes.Repeat([]byte{0xA5}, 32),
		Status:             "SCHEDULED",
		ScheduledAt:        &scheduledAt,
		DispatchDeadline:   acceptedAt.Add(10 * time.Minute),
		DispatchGeneration: 0,
		AttemptCount:       0,
		MaxAttempts:        3,
		LatestSequence:     2,
	}
	if err := insertMessage(ctx, db, validMessage, acceptedAt); err != nil {
		t.Fatalf("insert valid message: %v", err)
	}

	t.Run("scopes idempotency to tenant", func(t *testing.T) {
		duplicate := validMessage
		duplicate.ID = "20000000-0000-4000-8000-000000000002"
		assertPostgresError(
			t,
			insertMessage(ctx, db, duplicate, acceptedAt),
			"23505",
			"mail_messages_tenant_idempotency_unique",
		)

		otherTenant := validMessage
		otherTenant.ID = "20000000-0000-4000-8000-000000000003"
		otherTenant.TenantID = tenantTwo
		if err := insertMessage(ctx, db, otherTenant, acceptedAt); err != nil {
			t.Fatalf("same idempotency key in another tenant: %v", err)
		}
	})

	t.Run("rejects unknown message status", func(t *testing.T) {
		row := validMessage
		row.ID = "20000000-0000-4000-8000-000000000004"
		row.IdempotencyKey = "invalid-status"
		row.Status = "NOT_A_STATUS"
		assertPostgresError(
			t,
			insertMessage(ctx, db, row, acceptedAt),
			"23514",
			"mail_messages_status_valid",
		)
	})

	t.Run("rejects invalid attempt counters", func(t *testing.T) {
		row := validMessage
		row.ID = "20000000-0000-4000-8000-000000000005"
		row.IdempotencyKey = "invalid-attempts"
		row.Status = "SENDING"
		row.ScheduledAt = nil
		row.DispatchGeneration = 1
		row.AttemptCount = 4
		row.MaxAttempts = 3
		assertPostgresError(
			t,
			insertMessage(ctx, db, row, acceptedAt),
			"23514",
			"mail_messages_attempts_valid",
		)
	})

	t.Run("requires retry time for retry state", func(t *testing.T) {
		row := validMessage
		row.ID = "20000000-0000-4000-8000-000000000006"
		row.IdempotencyKey = "missing-retry-time"
		row.Status = "RETRY_SCHEDULED"
		row.ScheduledAt = nil
		row.NextAttemptAt = nil
		row.DispatchGeneration = 1
		row.AttemptCount = 1
		assertPostgresError(
			t,
			insertMessage(ctx, db, row, acceptedAt),
			"23514",
			"mail_messages_retry_time_present",
		)
	})

	t.Run("requires generation for queued state", func(t *testing.T) {
		row := validMessage
		row.ID = "20000000-0000-4000-8000-000000000008"
		row.IdempotencyKey = "missing-generation"
		row.Status = "QUEUED"
		row.ScheduledAt = nil
		row.DispatchGeneration = 0
		assertPostgresError(
			t,
			insertMessage(ctx, db, row, acceptedAt),
			"23514",
			"mail_messages_dispatch_generation_present",
		)
	})

	t.Run("requires attempt for sending state", func(t *testing.T) {
		row := validMessage
		row.ID = "20000000-0000-4000-8000-000000000009"
		row.IdempotencyKey = "missing-attempt"
		row.Status = "SENDING"
		row.ScheduledAt = nil
		row.DispatchGeneration = 1
		row.AttemptCount = 0
		assertPostgresError(
			t,
			insertMessage(ctx, db, row, acceptedAt),
			"23514",
			"mail_messages_attempt_present",
		)
	})

	t.Run("requires SHA-256 sized fingerprint", func(t *testing.T) {
		row := validMessage
		row.ID = "20000000-0000-4000-8000-000000000007"
		row.IdempotencyKey = "short-fingerprint"
		row.Fingerprint = bytes.Repeat([]byte{0xA5}, 31)
		assertPostgresError(
			t,
			insertMessage(ctx, db, row, acceptedAt),
			"23514",
			"mail_messages_payload_fingerprint_sha256",
		)
	})

	t.Run("deduplicates outbox event identity", func(t *testing.T) {
		const statement = `
			INSERT INTO outbox_events (
				id,
				aggregate_type,
				aggregate_id,
				event_type,
				aggregate_sequence,
				dispatch_generation,
				payload
			) VALUES ($1, 'MAIL_MESSAGE', $2, 'MESSAGE_DISPATCH_REQUESTED', 2, 1, $3)
		`
		payload := `{"message_id":"20000000-0000-4000-8000-000000000001","dispatch_generation":1}`
		if _, err := db.ExecContext(
			ctx,
			statement,
			"30000000-0000-4000-8000-000000000001",
			messageOne,
			payload,
		); err != nil {
			t.Fatalf("insert valid outbox event: %v", err)
		}
		_, err := db.ExecContext(
			ctx,
			statement,
			"30000000-0000-4000-8000-000000000002",
			messageOne,
			payload,
		)
		assertPostgresError(t, err, "23505", "outbox_events_identity_unique")
	})

	t.Run("requires object outbox payload", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO outbox_events (
				id,
				aggregate_type,
				aggregate_id,
				event_type,
				aggregate_sequence,
				dispatch_generation,
				payload
			) VALUES ($1, 'MAIL_MESSAGE', $2, 'MESSAGE_ACCEPTED', 1, 0, $3)
		`,
			"30000000-0000-4000-8000-000000000003",
			messageOne,
			`["not-an-object"]`,
		)
		assertPostgresError(t, err, "23514", "outbox_events_payload_object")
	})

	t.Run("requires paired outbox lease fields", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO outbox_events (
				id,
				aggregate_type,
				aggregate_id,
				event_type,
				aggregate_sequence,
				payload,
				lease_owner
			) VALUES ($1, 'MAIL_MESSAGE', $2, 'MESSAGE_STATUS_CHANGED', 3, $3, 'relay-1')
		`,
			"30000000-0000-4000-8000-000000000004",
			messageOne,
			`{}`,
		)
		assertPostgresError(t, err, "23514", "outbox_events_lease_fields_consistent")
	})

	t.Run("requires timestamp for published outbox event", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO outbox_events (
				id,
				aggregate_type,
				aggregate_id,
				event_type,
				aggregate_sequence,
				payload,
				status
			) VALUES ($1, 'MAIL_MESSAGE', $2, 'MESSAGE_STATUS_CHANGED', 4, $3, 'PUBLISHED')
		`,
			"30000000-0000-4000-8000-000000000005",
			messageOne,
			`{}`,
		)
		assertPostgresError(t, err, "23514", "outbox_events_published_fields_consistent")
	})

	t.Run("creates scheduler and relay indexes", func(t *testing.T) {
		want := map[string]bool{
			"mail_messages_scheduled_due_idx":     false,
			"mail_messages_retry_due_idx":         false,
			"mail_messages_tenant_accepted_idx":   false,
			"outbox_events_pending_available_idx": false,
			"outbox_events_pending_lease_idx":     false,
		}
		rows, err := db.QueryContext(ctx, `
			SELECT indexname
			FROM pg_indexes
			WHERE schemaname = current_schema()
		`)
		if err != nil {
			t.Fatalf("query indexes: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scan index name: %v", err)
			}
			if _, ok := want[name]; ok {
				want[name] = true
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate indexes: %v", err)
		}
		for name, found := range want {
			if !found {
				t.Errorf("index %q was not created", name)
			}
		}
	})

	if err := goose.DownContext(ctx, db, "sql"); err != nil {
		t.Fatalf("roll back migration: %v", err)
	}
	assertTableMissing(t, ctx, db, "tenants")
	assertTableMissing(t, ctx, db, "mail_messages")
	assertTableMissing(t, ctx, db, "outbox_events")

	if err := goose.UpContext(ctx, db, "sql"); err != nil {
		t.Fatalf("reapply migration after rollback: %v", err)
	}
	assertTablePresent(t, ctx, db, "tenants")
	assertTablePresent(t, ctx, db, "mail_messages")
	assertTablePresent(t, ctx, db, "outbox_events")
}

func insertTenant(t *testing.T, ctx context.Context, db *sql.DB, id, key string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO tenants (id, key, name, default_locale)
		VALUES ($1, $2, $3, 'zh-CN')
	`, id, key, key+" name"); err != nil {
		t.Fatalf("insert tenant %q: %v", key, err)
	}
}

func insertMessage(ctx context.Context, db *sql.DB, row messageRow, acceptedAt time.Time) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO mail_messages (
			id,
			tenant_id,
			idempotency_key,
			payload_fingerprint,
			category,
			priority,
			duplicate_risk_policy,
			status,
			scheduled_at,
			dispatch_deadline,
			next_attempt_at,
			dispatch_generation,
			attempt_count,
			max_attempts,
			latest_sequence,
			version,
			accepted_at,
			updated_at
		) VALUES (
			$1, $2, $3, $4, 'CRITICAL', 9, 'AVOID_DUPLICATE', $5, $6, $7, $8,
			$9, $10, $11, $12, 0, $13, $13
		)
	`,
		row.ID,
		row.TenantID,
		row.IdempotencyKey,
		row.Fingerprint,
		row.Status,
		row.ScheduledAt,
		row.DispatchDeadline,
		row.NextAttemptAt,
		row.DispatchGeneration,
		row.AttemptCount,
		row.MaxAttempts,
		row.LatestSequence,
		acceptedAt,
	)
	return err
}

func assertPostgresError(t *testing.T, err error, code, constraint string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected PostgreSQL error %s for constraint %q", code, constraint)
	}
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) {
		t.Fatalf("error type = %T, want *pgconn.PgError: %v", err, err)
	}
	if pgError.Code != code {
		t.Errorf("SQLSTATE = %q, want %q: %v", pgError.Code, code, err)
	}
	if pgError.ConstraintName != constraint {
		t.Errorf("constraint = %q, want %q: %v", pgError.ConstraintName, constraint, err)
	}
}

func assertTableMissing(t *testing.T, ctx context.Context, db *sql.DB, table string) {
	t.Helper()
	var relation sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT to_regclass($1)", "public."+table).Scan(&relation); err != nil {
		t.Fatalf("query table %q: %v", table, err)
	}
	if relation.Valid {
		t.Fatalf("table %q still exists as %q", table, relation.String)
	}
}

func assertTablePresent(t *testing.T, ctx context.Context, db *sql.DB, table string) {
	t.Helper()
	var relation sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT to_regclass($1)", "public."+table).Scan(&relation); err != nil {
		t.Fatalf("query table %q: %v", table, err)
	}
	if !relation.Valid {
		t.Fatalf("table %q does not exist", table)
	}
	want := fmt.Sprintf("%s", table)
	if relation.String != want {
		t.Fatalf("table relation = %q, want %q", relation.String, want)
	}
}
