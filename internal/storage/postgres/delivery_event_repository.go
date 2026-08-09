package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/jackc/pgx/v5"
)

type DeliveryEventRepository struct {
	db DBTX
}

var _ ports.DeliveryEventRepository = (*DeliveryEventRepository)(nil)

func NewDeliveryEventRepository(db DBTX) *DeliveryEventRepository {
	if db == nil {
		panic("postgres: nil DBTX")
	}
	return &DeliveryEventRepository{db: db}
}

func (r *DeliveryEventRepository) Append(
	ctx context.Context,
	events []ports.DeliveryEvent,
) error {
	for _, event := range events {
		if err := event.Validate(); err != nil {
			return err
		}
		arguments := deliveryEventArguments(event)
		var insertedID string
		err := r.db.QueryRow(ctx, insertDeliveryEventQuery, arguments...).Scan(&insertedID)
		if err == nil {
			if insertedID != event.ID {
				return fmt.Errorf("%w: database returned another event id", ports.ErrDeliveryEventConflict)
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return mapStorageError(ctx, ports.ErrDeliveryEventRepository, "append event", err)
		}
		var matches bool
		if err := r.db.QueryRow(ctx, deliveryEventMatchesQuery, arguments...).Scan(&matches); err != nil {
			return mapStorageError(ctx, ports.ErrDeliveryEventRepository, "compare event", err)
		}
		if !matches {
			return ports.ErrDeliveryEventConflict
		}
	}
	return nil
}

func deliveryEventArguments(event ports.DeliveryEvent) []any {
	var failureCategory, failureCode, failureRetryable any
	if event.Failure != nil {
		failureCategory = string(event.Failure.Category)
		failureCode = event.Failure.Code
		failureRetryable = event.Failure.Retryable
	}
	return []any{
		event.ID,
		event.TenantID,
		event.MessageID,
		event.IdempotencyKey,
		string(event.Status),
		int64(event.Sequence),
		int32(event.AttemptNumber),
		nullableString(event.ProviderMessageID),
		failureCategory,
		failureCode,
		failureRetryable,
		event.OccurredAt.UTC(),
	}
}
