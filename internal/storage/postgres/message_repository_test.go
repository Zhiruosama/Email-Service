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
