package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/jackc/pgx/v5"
)

// OutboxDeliveryRepository is transaction-only: claims and fenced state
// changes must commit before or after transport I/O, never around it.
type OutboxDeliveryRepository struct {
	tx pgx.Tx
}

var _ ports.OutboxDeliveryRepository = (*OutboxDeliveryRepository)(nil)

func NewOutboxDeliveryRepository(tx pgx.Tx) *OutboxDeliveryRepository {
	if tx == nil {
		panic("postgres: nil transaction")
	}
	return &OutboxDeliveryRepository{tx: tx}
}

func (r *OutboxDeliveryRepository) ClaimPending(
	ctx context.Context,
	query ports.OutboxClaimQuery,
) (ports.OutboxClaimBatch, error) {
	if err := query.Validate(); err != nil {
		return ports.OutboxClaimBatch{}, err
	}

	rows, err := r.tx.Query(
		ctx,
		claimPendingOutboxQuery,
		int32(query.Limit),
		query.LeaseToken,
		query.LeaseDuration.Milliseconds(),
	)
	if err != nil {
		return ports.OutboxClaimBatch{}, mapStorageError(
			ctx,
			ports.ErrOutboxDeliveryRepository,
			"claim pending outbox events",
			err,
		)
	}
	defer rows.Close()

	events := make([]ports.LeasedOutboxEvent, 0, query.Limit)
	var evaluatedAt time.Time
	for rows.Next() {
		leased, rowEvaluatedAt, scanErr := scanLeasedOutboxEvent(ctx, rows)
		if scanErr != nil {
			return ports.OutboxClaimBatch{}, scanErr
		}
		if leased.LeaseToken != query.LeaseToken {
			return ports.OutboxClaimBatch{}, corruptOutboxDeliveryError(
				"claimed event has an unexpected lease token",
			)
		}
		if evaluatedAt.IsZero() {
			evaluatedAt = rowEvaluatedAt
		} else if !evaluatedAt.Equal(rowEvaluatedAt) {
			return ports.OutboxClaimBatch{}, corruptOutboxDeliveryError(
				"database returned inconsistent claim timestamps",
			)
		}
		events = append(events, leased)
	}
	if err := rows.Err(); err != nil {
		return ports.OutboxClaimBatch{}, mapStorageError(
			ctx,
			ports.ErrOutboxDeliveryRepository,
			"iterate claimed outbox events",
			err,
		)
	}
	return ports.OutboxClaimBatch{EvaluatedAt: evaluatedAt, Events: events}, nil
}

func (r *OutboxDeliveryRepository) MarkPublished(
	ctx context.Context,
	reference ports.OutboxLeaseReference,
) error {
	if err := reference.Validate(); err != nil {
		return err
	}
	var publishedAt time.Time
	err := r.tx.QueryRow(
		ctx,
		markOutboxPublishedQuery,
		reference.EventID,
		reference.LeaseToken,
		int32(reference.AttemptNumber),
	).Scan(&publishedAt)
	return mapOutboxCompletionError(ctx, "mark outbox published", err)
}

func (r *OutboxDeliveryRepository) Reschedule(
	ctx context.Context,
	command ports.RescheduleOutboxCommand,
) error {
	if err := command.Validate(); err != nil {
		return err
	}
	var availableAt time.Time
	err := r.tx.QueryRow(
		ctx,
		rescheduleOutboxQuery,
		command.Lease.EventID,
		command.Lease.LeaseToken,
		int32(command.Lease.AttemptNumber),
		command.Delay.Milliseconds(),
		command.ErrorCode,
	).Scan(&availableAt)
	return mapOutboxCompletionError(ctx, "reschedule outbox event", err)
}

func (r *OutboxDeliveryRepository) DeadLetter(
	ctx context.Context,
	command ports.DeadLetterOutboxCommand,
) error {
	if err := command.Validate(); err != nil {
		return err
	}
	var eventID string
	err := r.tx.QueryRow(
		ctx,
		deadLetterOutboxQuery,
		command.Lease.EventID,
		command.Lease.LeaseToken,
		int32(command.Lease.AttemptNumber),
		command.ErrorCode,
	).Scan(&eventID)
	return mapOutboxCompletionError(ctx, "dead-letter outbox event", err)
}

func scanLeasedOutboxEvent(
	ctx context.Context,
	row rowScanner,
) (ports.LeasedOutboxEvent, time.Time, error) {
	var (
		eventID            string
		aggregateType      string
		aggregateID        string
		eventType          string
		aggregateSequence  int64
		dispatchGeneration int64
		payload            []byte
		leaseToken         string
		leaseUntil         time.Time
		attemptCount       int32
		evaluatedAt        time.Time
	)
	if err := row.Scan(
		&eventID,
		&aggregateType,
		&aggregateID,
		&eventType,
		&aggregateSequence,
		&dispatchGeneration,
		&payload,
		&leaseToken,
		&leaseUntil,
		&attemptCount,
		&evaluatedAt,
	); err != nil {
		return ports.LeasedOutboxEvent{}, time.Time{}, mapStorageError(
			ctx,
			ports.ErrOutboxDeliveryRepository,
			"scan claimed outbox event",
			err,
		)
	}
	if aggregateSequence <= 0 || dispatchGeneration < 0 || attemptCount < 0 || attemptCount == math.MaxInt32 {
		return ports.LeasedOutboxEvent{}, time.Time{}, corruptOutboxDeliveryError(
			"persisted counters are outside the claimable range",
		)
	}
	event := ports.OutboxEvent{
		ID:                 eventID,
		AggregateType:      aggregateType,
		AggregateID:        aggregateID,
		EventType:          eventType,
		AggregateSequence:  uint64(aggregateSequence),
		DispatchGeneration: uint64(dispatchGeneration),
		Payload:            payload,
	}
	leased := ports.LeasedOutboxEvent{
		Event:         event,
		LeaseToken:    leaseToken,
		LeaseUntil:    leaseUntil.UTC(),
		AttemptNumber: uint32(attemptCount) + 1,
	}
	if err := leased.Validate(); err != nil {
		return ports.LeasedOutboxEvent{}, time.Time{}, corruptOutboxDeliveryError(
			fmt.Sprintf("claimed event failed validation: %v", err),
		)
	}
	evaluatedAt = evaluatedAt.UTC()
	if evaluatedAt.IsZero() || !leased.LeaseUntil.After(evaluatedAt) {
		return ports.LeasedOutboxEvent{}, time.Time{}, corruptOutboxDeliveryError(
			"claim timestamps are invalid",
		)
	}
	return leased, evaluatedAt, nil
}

func mapOutboxCompletionError(ctx context.Context, operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.ErrOutboxLeaseLost
	}
	return mapStorageError(ctx, ports.ErrOutboxDeliveryRepository, operation, err)
}

func corruptOutboxDeliveryError(detail string) error {
	return fmt.Errorf("%w: %s", ports.ErrCorruptOutboxDelivery, detail)
}
