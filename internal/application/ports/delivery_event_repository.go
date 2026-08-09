package ports

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"github.com/google/uuid"
)

var (
	ErrInvalidDeliveryEvent    = errors.New("invalid delivery event")
	ErrDeliveryEventConflict   = errors.New("delivery event identity conflict")
	ErrDeliveryEventRepository = errors.New("delivery event repository failure")
)

// DeliveryEvent is an immutable, sanitized lifecycle fact. Its ID is also the
// event_id used by Outbox and downstream at-least-once notification handling.
type DeliveryEvent struct {
	ID                string
	TenantID          string
	MessageID         string
	IdempotencyKey    string
	Status            message.Status
	Sequence          uint64
	AttemptNumber     uint32
	ProviderMessageID string
	Failure           *message.Failure
	OccurredAt        time.Time
}

func (e DeliveryEvent) Validate() error {
	if _, err := uuid.Parse(e.ID); err != nil {
		return fmt.Errorf("%w: event id must be a UUID", ErrInvalidDeliveryEvent)
	}
	if _, err := uuid.Parse(e.TenantID); err != nil {
		return fmt.Errorf("%w: tenant id must be a UUID", ErrInvalidDeliveryEvent)
	}
	if _, err := uuid.Parse(e.MessageID); err != nil {
		return fmt.Errorf("%w: message id must be a UUID", ErrInvalidDeliveryEvent)
	}
	if strings.TrimSpace(e.IdempotencyKey) == "" || len(e.IdempotencyKey) > 255 {
		return fmt.Errorf("%w: idempotency key must contain 1..255 bytes", ErrInvalidDeliveryEvent)
	}
	if !e.Status.Valid() {
		return fmt.Errorf("%w: status is invalid", ErrInvalidDeliveryEvent)
	}
	if e.Sequence == 0 || e.Sequence > math.MaxInt64 || e.AttemptNumber > math.MaxInt32 {
		return fmt.Errorf("%w: counters exceed database range", ErrInvalidDeliveryEvent)
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred time is required", ErrInvalidDeliveryEvent)
	}
	if strings.TrimSpace(e.ProviderMessageID) != e.ProviderMessageID || len(e.ProviderMessageID) > 512 {
		return fmt.Errorf("%w: provider message id is invalid", ErrInvalidDeliveryEvent)
	}
	if e.Failure != nil {
		if err := e.Failure.Validate(); err != nil {
			return fmt.Errorf("%w: failure is invalid", ErrInvalidDeliveryEvent)
		}
	}
	return nil
}

type DeliveryEventRepository interface {
	Append(context.Context, []DeliveryEvent) error
}
