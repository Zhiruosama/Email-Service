package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type DeliveryEventRepository struct {
	db DBTX
}

var _ ports.DeliveryEventRepository = (*DeliveryEventRepository)(nil)
var _ ports.DeliveryEventReader = (*DeliveryEventRepository)(nil)

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

func (r *DeliveryEventRepository) GetByID(
	ctx context.Context,
	eventID string,
) (ports.PersistedDeliveryEvent, error) {
	if _, err := uuid.Parse(eventID); err != nil {
		return ports.PersistedDeliveryEvent{}, fmt.Errorf(
			"%w: event id must be a UUID",
			ports.ErrInvalidDeliveryEvent,
		)
	}
	event, err := scanDeliveryEvent(r.db.QueryRow(ctx, getDeliveryEventByIDQuery, eventID))
	if err == nil {
		return event, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.PersistedDeliveryEvent{}, ports.ErrDeliveryEventNotFound
	}
	if errors.Is(err, ports.ErrCorruptDeliveryEvent) {
		return ports.PersistedDeliveryEvent{}, err
	}
	return ports.PersistedDeliveryEvent{}, mapStorageError(
		ctx,
		ports.ErrDeliveryEventRepository,
		"read event",
		err,
	)
}

func scanDeliveryEvent(row rowScanner) (ports.PersistedDeliveryEvent, error) {
	var (
		eventID           string
		tenantID          string
		messageID         string
		idempotencyKey    string
		status            string
		sequence          int64
		attemptNumber     int32
		providerMessageID pgtype.Text
		failureCategory   pgtype.Text
		failureCode       pgtype.Text
		failureRetryable  pgtype.Bool
		occurredAt        time.Time
		observedAt        time.Time
	)
	if err := row.Scan(
		&eventID,
		&tenantID,
		&messageID,
		&idempotencyKey,
		&status,
		&sequence,
		&attemptNumber,
		&providerMessageID,
		&failureCategory,
		&failureCode,
		&failureRetryable,
		&occurredAt,
		&observedAt,
	); err != nil {
		return ports.PersistedDeliveryEvent{}, err
	}
	if sequence <= 0 || attemptNumber < 0 {
		return ports.PersistedDeliveryEvent{}, corruptDeliveryEventError(
			"persisted counters are outside the domain range",
			nil,
		)
	}
	failure, err := scanDeliveryEventFailure(
		failureCategory,
		failureCode,
		failureRetryable,
	)
	if err != nil {
		return ports.PersistedDeliveryEvent{}, err
	}
	event := ports.PersistedDeliveryEvent{
		DeliveryEvent: ports.DeliveryEvent{
			ID:                eventID,
			TenantID:          tenantID,
			MessageID:         messageID,
			IdempotencyKey:    idempotencyKey,
			Status:            message.Status(status),
			Sequence:          uint64(sequence),
			AttemptNumber:     uint32(attemptNumber),
			ProviderMessageID: textValue(providerMessageID),
			Failure:           failure,
			OccurredAt:        occurredAt.UTC(),
		},
		ObservedAt: observedAt.UTC(),
	}
	if err := event.Validate(); err != nil {
		return ports.PersistedDeliveryEvent{}, corruptDeliveryEventError(
			"persisted event is invalid",
			err,
		)
	}
	return event, nil
}

func scanDeliveryEventFailure(
	category, code pgtype.Text,
	retryable pgtype.Bool,
) (*message.Failure, error) {
	if !category.Valid && !code.Valid && !retryable.Valid {
		return nil, nil
	}
	if !category.Valid || !code.Valid || !retryable.Valid {
		return nil, corruptDeliveryEventError(
			"failure fields are only partially present",
			nil,
		)
	}
	return &message.Failure{
		Category:  message.FailureCategory(category.String),
		Code:      code.String,
		Retryable: retryable.Bool,
	}, nil
}

type corruptDeliveryEventRecordError struct {
	detail string
	cause  error
}

func (e *corruptDeliveryEventRecordError) Error() string {
	return fmt.Sprintf("%s: %s", ports.ErrCorruptDeliveryEvent, e.detail)
}

func (e *corruptDeliveryEventRecordError) Unwrap() error {
	return ports.ErrCorruptDeliveryEvent
}

func (e *corruptDeliveryEventRecordError) Cause() error { return e.cause }

func corruptDeliveryEventError(detail string, cause error) error {
	return &corruptDeliveryEventRecordError{detail: detail, cause: cause}
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
