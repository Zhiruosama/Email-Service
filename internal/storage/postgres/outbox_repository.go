package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type OutboxRepository struct {
	db DBTX
}

var _ ports.OutboxRepository = (*OutboxRepository)(nil)

func NewOutboxRepository(db DBTX) *OutboxRepository {
	if db == nil {
		panic("postgres: nil DBTX")
	}
	return &OutboxRepository{db: db}
}

// Append expects to run inside a caller-owned transaction when more than one
// event must be atomic. All events are validated before the first SQL write.
func (r *OutboxRepository) Append(ctx context.Context, events []ports.OutboxEvent) error {
	arguments := make([]outboxArguments, len(events))
	for index, event := range events {
		if err := event.Validate(); err != nil {
			return err
		}
		if event.AggregateSequence > math.MaxInt64 || event.DispatchGeneration > math.MaxInt64 {
			return fmt.Errorf("%w: counters exceed PostgreSQL BIGINT range", ports.ErrInvalidOutboxEvent)
		}
		arguments[index] = makeOutboxArguments(event)
	}

	for _, eventArguments := range arguments {
		var persistedID string
		err := r.db.QueryRow(ctx, insertOutboxEventQuery, eventArguments.insert()...).Scan(&persistedID)
		if err == nil {
			continue
		}
		if errors.Is(err, pgx.ErrNoRows) {
			matches, compareErr := r.payloadMatches(ctx, eventArguments)
			if compareErr != nil {
				return compareErr
			}
			if !matches {
				return ports.ErrOutboxConflict
			}
			continue
		}

		var pgError *pgconn.PgError
		if errors.As(err, &pgError) && pgError.ConstraintName == "outbox_events_pkey" {
			return ports.ErrOutboxIDConflict
		}
		return mapStorageError(ctx, ports.ErrOutboxRepository, "append outbox event", err)
	}
	return nil
}

func (r *OutboxRepository) payloadMatches(
	ctx context.Context,
	arguments outboxArguments,
) (bool, error) {
	var matches bool
	if err := r.db.QueryRow(
		ctx,
		outboxPayloadMatchesQuery,
		arguments.identityAndPayload()...,
	).Scan(&matches); err != nil {
		return false, mapStorageError(ctx, ports.ErrOutboxRepository, "compare outbox payload", err)
	}
	return matches, nil
}

type outboxArguments struct {
	id                 string
	aggregateType      string
	aggregateID        string
	eventType          string
	aggregateSequence  int64
	dispatchGeneration int64
	payload            string
}

func makeOutboxArguments(event ports.OutboxEvent) outboxArguments {
	return outboxArguments{
		id:                 event.ID,
		aggregateType:      event.AggregateType,
		aggregateID:        event.AggregateID,
		eventType:          event.EventType,
		aggregateSequence:  int64(event.AggregateSequence),
		dispatchGeneration: int64(event.DispatchGeneration),
		payload:            string(event.Payload),
	}
}

func (a outboxArguments) insert() []any {
	return []any{
		a.id,
		a.aggregateType,
		a.aggregateID,
		a.eventType,
		a.aggregateSequence,
		a.dispatchGeneration,
		a.payload,
	}
}

func (a outboxArguments) identityAndPayload() []any {
	return []any{
		a.aggregateType,
		a.aggregateID,
		a.eventType,
		a.aggregateSequence,
		a.dispatchGeneration,
		a.payload,
	}
}
