package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type stubDBTX struct {
	row pgx.Row
}

func (s stubDBTX) QueryRow(context.Context, string, ...any) pgx.Row {
	return s.row
}

type errorRow struct {
	err error
}

func (r errorRow) Scan(...any) error { return r.err }

func TestRepositoryHidesDriverErrorFromUnwrapChain(t *testing.T) {
	driverError := &pgconn.PgError{Code: "08006", Message: "connection failure"}
	repository := NewMessageRepository(stubDBTX{row: errorRow{err: driverError}})

	_, err := repository.GetByID(
		context.Background(),
		"50000000-0000-4000-8000-000000000001",
	)
	if !errors.Is(err, ports.ErrMessageRepository) {
		t.Fatalf("GetByID() error = %v, want ErrMessageRepository", err)
	}
	var leaked *pgconn.PgError
	if errors.As(err, &leaked) {
		t.Fatal("driver error leaked through errors.Unwrap")
	}

	type causer interface{ Cause() error }
	var internalCause causer
	if !errors.As(err, &internalCause) || !errors.As(internalCause.Cause(), &leaked) {
		t.Fatal("internal cause is unavailable to observability code")
	}
}

func TestRepositoryPreservesContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	repository := NewMessageRepository(stubDBTX{row: errorRow{err: errors.New("driver stopped")}})

	_, err := repository.GetByID(ctx, "50000000-0000-4000-8000-000000000001")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetByID() error = %v, want context.Canceled", err)
	}
}

func TestNewMessageRepositoryRejectsNilDBTX(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewMessageRepository(nil) did not panic")
		}
	}()
	NewMessageRepository(nil)
}

func TestOutboxRepositoryHidesDriverErrorFromUnwrapChain(t *testing.T) {
	driverError := &pgconn.PgError{Code: "08006", Message: "connection failure"}
	repository := NewOutboxRepository(stubDBTX{row: errorRow{err: driverError}})
	event := ports.OutboxEvent{
		ID:                "60000000-0000-4000-8000-000000000001",
		AggregateType:     ports.OutboxAggregateMailMessage,
		AggregateID:       "50000000-0000-4000-8000-000000000001",
		EventType:         "TEST_EVENT",
		AggregateSequence: 1,
		Payload:           []byte(`{"schema_version":1}`),
	}

	err := repository.Append(context.Background(), []ports.OutboxEvent{event})
	if !errors.Is(err, ports.ErrOutboxRepository) {
		t.Fatalf("Append() error = %v, want ErrOutboxRepository", err)
	}
	var leaked *pgconn.PgError
	if errors.As(err, &leaked) {
		t.Fatal("driver error leaked through errors.Unwrap")
	}
}

func TestConstructorsRejectNilDependencies(t *testing.T) {
	t.Run("outbox repository", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("NewOutboxRepository(nil) did not panic")
			}
		}()
		NewOutboxRepository(nil)
	})

	t.Run("transaction manager", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("NewTransactionManager(nil) did not panic")
			}
		}()
		NewTransactionManager(nil)
	})

	t.Run("due message repository", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("NewDueMessageRepository(nil) did not panic")
			}
		}()
		NewDueMessageRepository(nil)
	})

	t.Run("outbox delivery repository", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("NewOutboxDeliveryRepository(nil) did not panic")
			}
		}()
		NewOutboxDeliveryRepository(nil)
	})

	t.Run("delivery attempt repository", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("NewDeliveryAttemptRepository(nil) did not panic")
			}
		}()
		NewDeliveryAttemptRepository(nil)
	})

	t.Run("transaction clock", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("NewTransactionClock(nil) did not panic")
			}
		}()
		NewTransactionClock(nil)
	})
}
