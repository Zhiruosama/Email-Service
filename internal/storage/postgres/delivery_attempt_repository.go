package postgres

import (
	"context"
	"errors"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DeliveryAttemptRepository struct {
	db DBTX
}

var _ ports.DeliveryAttemptRepository = (*DeliveryAttemptRepository)(nil)

func NewDeliveryAttemptRepository(db DBTX) *DeliveryAttemptRepository {
	if db == nil {
		panic("postgres: nil DBTX")
	}
	return &DeliveryAttemptRepository{db: db}
}

func (r *DeliveryAttemptRepository) CreateStarted(
	ctx context.Context,
	attempt ports.StartedDeliveryAttempt,
) error {
	if err := attempt.Validate(); err != nil {
		return err
	}
	var attemptID string
	err := r.db.QueryRow(
		ctx,
		insertStartedDeliveryAttemptQuery,
		attempt.ID,
		attempt.MessageID,
		int32(attempt.AttemptNumber),
		int64(attempt.DispatchGeneration),
		attempt.ProviderKey,
		attempt.StartedAt.UTC(),
	).Scan(&attemptID)
	if err == nil {
		return nil
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) && pgError.Code == "23505" {
		return ports.ErrDeliveryAttemptConflict
	}
	return mapStorageError(
		ctx,
		ports.ErrDeliveryAttemptRepository,
		"create started delivery attempt",
		err,
	)
}

func (r *DeliveryAttemptRepository) Complete(
	ctx context.Context,
	completion ports.CompleteDeliveryAttempt,
) error {
	if err := completion.Validate(); err != nil {
		return err
	}

	var (
		providerMessageID any
		failureCategory   any
		failureCode       any
		failureRetryable  any
	)
	if completion.ProviderMessageID != "" {
		providerMessageID = completion.ProviderMessageID
	}
	if completion.Failure != nil {
		failureCategory = string(completion.Failure.Category)
		failureCode = completion.Failure.Code
		failureRetryable = completion.Failure.Retryable
	}

	var attemptID string
	err := r.db.QueryRow(
		ctx,
		completeDeliveryAttemptQuery,
		completion.AttemptID,
		string(completion.Status),
		completion.FinishedAt.UTC(),
		providerMessageID,
		failureCategory,
		failureCode,
		failureRetryable,
	).Scan(&attemptID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.ErrDeliveryAttemptConflict
	}
	if err != nil {
		return mapStorageError(
			ctx,
			ports.ErrDeliveryAttemptRepository,
			"complete delivery attempt",
			err,
		)
	}
	return nil
}
