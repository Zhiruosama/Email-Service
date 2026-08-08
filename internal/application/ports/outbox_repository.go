package ports

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const (
	OutboxAggregateMailMessage = "MAIL_MESSAGE"
	maxOutboxPayloadBytes      = 64 * 1024
)

var (
	ErrInvalidOutboxEvent = errors.New("invalid outbox event")
	ErrOutboxConflict     = errors.New("outbox identity reused with different payload")
	ErrOutboxIDConflict   = errors.New("outbox event id already exists")
	ErrOutboxRepository   = errors.New("outbox repository failure")
	ErrTransaction        = errors.New("transaction failure")
)

// OutboxEvent is a safe, transport-neutral event. Payload must be a bounded
// JSON object and must never contain message bodies, addresses, template
// variables, verification codes, or provider credentials.
type OutboxEvent struct {
	ID                 string
	AggregateType      string
	AggregateID        string
	EventType          string
	AggregateSequence  uint64
	DispatchGeneration uint64
	Payload            json.RawMessage
}

func (e OutboxEvent) Validate() error {
	if _, err := uuid.Parse(e.ID); err != nil {
		return fmt.Errorf("%w: id must be a UUID", ErrInvalidOutboxEvent)
	}
	if strings.TrimSpace(e.AggregateType) == "" || len(e.AggregateType) > 64 {
		return fmt.Errorf("%w: aggregate type must contain 1..64 bytes", ErrInvalidOutboxEvent)
	}
	if _, err := uuid.Parse(e.AggregateID); err != nil {
		return fmt.Errorf("%w: aggregate id must be a UUID", ErrInvalidOutboxEvent)
	}
	if strings.TrimSpace(e.EventType) == "" || len(e.EventType) > 128 {
		return fmt.Errorf("%w: event type must contain 1..128 bytes", ErrInvalidOutboxEvent)
	}
	if e.AggregateSequence == 0 {
		return fmt.Errorf("%w: aggregate sequence must be positive", ErrInvalidOutboxEvent)
	}
	if len(e.Payload) == 0 || len(e.Payload) > maxOutboxPayloadBytes || !json.Valid(e.Payload) {
		return fmt.Errorf("%w: payload must be valid JSON within 64 KiB", ErrInvalidOutboxEvent)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(e.Payload, &object); err != nil || object == nil {
		return fmt.Errorf("%w: payload must be a JSON object", ErrInvalidOutboxEvent)
	}
	return nil
}

type OutboxRepository interface {
	Append(context.Context, []OutboxEvent) error
}

// UnitOfWork exposes repositories bound to one infrastructure transaction.
// Application code can coordinate atomic writes without importing pgx.
type UnitOfWork interface {
	Messages() MessageRepository
	Outbox() OutboxRepository
}

type TransactionFunc func(UnitOfWork) error

type Transactor interface {
	WithinTransaction(context.Context, TransactionFunc) error
}
