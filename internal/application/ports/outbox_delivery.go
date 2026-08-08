package ports

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	MaxOutboxDeliveryBatchSize uint32 = 1000
	maxOutboxLeaseDuration            = time.Hour
	maxOutboxRetryDelay               = 24 * time.Hour
)

var (
	ErrInvalidOutboxDelivery     = errors.New("invalid outbox delivery operation")
	ErrCorruptOutboxDelivery     = errors.New("corrupt persisted outbox delivery")
	ErrOutboxDeliveryRepository  = errors.New("outbox delivery repository failure")
	ErrOutboxLeaseLost           = errors.New("outbox lease is no longer owned")
	ErrInvalidOutboxPublishError = errors.New("invalid outbox publish error")
)

type OutboxClaimQuery struct {
	LeaseToken    string
	LeaseDuration time.Duration
	Limit         uint32
}

func (q OutboxClaimQuery) Validate() error {
	if err := validateLeaseToken(q.LeaseToken); err != nil {
		return err
	}
	if q.LeaseDuration < time.Second || q.LeaseDuration > maxOutboxLeaseDuration {
		return fmt.Errorf(
			"%w: lease duration must be in range 1s..%s",
			ErrInvalidOutboxDelivery,
			maxOutboxLeaseDuration,
		)
	}
	if q.Limit == 0 || q.Limit > MaxOutboxDeliveryBatchSize {
		return fmt.Errorf(
			"%w: limit must be in range 1..%d",
			ErrInvalidOutboxDelivery,
			MaxOutboxDeliveryBatchSize,
		)
	}
	return nil
}

type LeasedOutboxEvent struct {
	Event         OutboxEvent
	LeaseToken    string
	LeaseUntil    time.Time
	AttemptNumber uint32
}

func (e LeasedOutboxEvent) Validate() error {
	if err := e.Event.Validate(); err != nil {
		return err
	}
	if err := validateLeaseToken(e.LeaseToken); err != nil {
		return err
	}
	if e.LeaseUntil.IsZero() {
		return fmt.Errorf("%w: lease expiry is required", ErrInvalidOutboxDelivery)
	}
	if e.AttemptNumber == 0 || e.AttemptNumber > math.MaxInt32 {
		return fmt.Errorf(
			"%w: attempt number must fit a positive PostgreSQL INTEGER",
			ErrInvalidOutboxDelivery,
		)
	}
	return nil
}

type OutboxClaimBatch struct {
	EvaluatedAt time.Time
	Events      []LeasedOutboxEvent
}

type OutboxLeaseReference struct {
	EventID       string
	LeaseToken    string
	AttemptNumber uint32
}

func (r OutboxLeaseReference) Validate() error {
	if _, err := uuid.Parse(r.EventID); err != nil {
		return fmt.Errorf("%w: event id must be a UUID", ErrInvalidOutboxDelivery)
	}
	if err := validateLeaseToken(r.LeaseToken); err != nil {
		return err
	}
	if r.AttemptNumber == 0 || r.AttemptNumber > math.MaxInt32 {
		return fmt.Errorf(
			"%w: attempt number must fit a positive PostgreSQL INTEGER",
			ErrInvalidOutboxDelivery,
		)
	}
	return nil
}

type RescheduleOutboxCommand struct {
	Lease     OutboxLeaseReference
	Delay     time.Duration
	ErrorCode string
}

func (c RescheduleOutboxCommand) Validate() error {
	if err := c.Lease.Validate(); err != nil {
		return err
	}
	if c.Delay < 0 || c.Delay > maxOutboxRetryDelay {
		return fmt.Errorf(
			"%w: retry delay must be in range 0..%s",
			ErrInvalidOutboxDelivery,
			maxOutboxRetryDelay,
		)
	}
	if !validStableCode(c.ErrorCode) {
		return fmt.Errorf(
			"%w: error code must be a stable 1..128 byte identifier",
			ErrInvalidOutboxDelivery,
		)
	}
	return nil
}

type DeadLetterOutboxCommand struct {
	Lease     OutboxLeaseReference
	ErrorCode string
}

func (c DeadLetterOutboxCommand) Validate() error {
	if err := c.Lease.Validate(); err != nil {
		return err
	}
	if !validStableCode(c.ErrorCode) {
		return fmt.Errorf(
			"%w: error code must be a stable 1..128 byte identifier",
			ErrInvalidOutboxDelivery,
		)
	}
	return nil
}

// OutboxDeliveryRepository manages the database side of a cross-transaction
// publish attempt. Every mutation is fenced by the unique lease token and the
// expected attempt number.
type OutboxDeliveryRepository interface {
	ClaimPending(context.Context, OutboxClaimQuery) (OutboxClaimBatch, error)
	MarkPublished(context.Context, OutboxLeaseReference) error
	Reschedule(context.Context, RescheduleOutboxCommand) error
	DeadLetter(context.Context, DeadLetterOutboxCommand) error
}

type OutboxPublication struct {
	Event         OutboxEvent
	AttemptNumber uint32
}

type OutboxPublisher interface {
	// Publish returns nil only after the downstream transport has confirmed
	// ownership. A returned error must not be interpreted as definite absence;
	// the transport may have accepted the event before the result was lost.
	Publish(context.Context, OutboxPublication) error
}

// OutboxPublishError is a sanitized transport failure. Cause is retained for
// internal observability and deliberately excluded from the unwrap chain.
type OutboxPublishError struct {
	Code      string
	Retryable bool
	cause     error
}

func NewOutboxPublishError(code string, retryable bool, cause error) *OutboxPublishError {
	return &OutboxPublishError{Code: code, Retryable: retryable, cause: cause}
}

func (e *OutboxPublishError) Error() string {
	if e == nil {
		return "outbox publish failed"
	}
	return fmt.Sprintf("outbox publish failed: %s", e.Code)
}

func (e *OutboxPublishError) Cause() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *OutboxPublishError) Validate() error {
	if e == nil || !validStableCode(e.Code) {
		return fmt.Errorf(
			"%w: code must be a stable 1..128 byte identifier",
			ErrInvalidOutboxPublishError,
		)
	}
	return nil
}

func validateLeaseToken(value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed != value || len(value) > 255 || strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf(
			"%w: lease token must contain 1..255 bytes without surrounding whitespace",
			ErrInvalidOutboxDelivery,
		)
	}
	return nil
}

func validStableCode(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range []byte(value) {
		if (character >= 'A' && character <= 'Z') ||
			(character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') ||
			character == '.' || character == '_' || character == ':' || character == '-' {
			continue
		}
		return false
	}
	return true
}
