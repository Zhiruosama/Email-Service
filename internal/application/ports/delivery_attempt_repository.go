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
	ErrInvalidDeliveryAttempt    = errors.New("invalid delivery attempt")
	ErrDeliveryAttemptConflict   = errors.New("delivery attempt changed or already exists")
	ErrDeliveryAttemptRepository = errors.New("delivery attempt repository failure")
)

type DeliveryAttemptStatus string

const (
	DeliveryAttemptStarted           DeliveryAttemptStatus = "STARTED"
	DeliveryAttemptProviderAccepted  DeliveryAttemptStatus = "PROVIDER_ACCEPTED"
	DeliveryAttemptFailed            DeliveryAttemptStatus = "FAILED"
	DeliveryAttemptSubmissionUnknown DeliveryAttemptStatus = "SUBMISSION_UNKNOWN"
)

func (s DeliveryAttemptStatus) Valid() bool {
	switch s {
	case DeliveryAttemptStarted,
		DeliveryAttemptProviderAccepted,
		DeliveryAttemptFailed,
		DeliveryAttemptSubmissionUnknown:
		return true
	default:
		return false
	}
}

// StartedDeliveryAttempt is inserted in the same transaction that advances a
// Message to SENDING. A committed row proves that a provider call may be in
// flight even if the worker process disappears immediately afterward.
type StartedDeliveryAttempt struct {
	ID                 string
	MessageID          string
	AttemptNumber      uint32
	DispatchGeneration uint64
	ProviderKey        string
	StartedAt          time.Time
}

func (a StartedDeliveryAttempt) Validate() error {
	if _, err := uuid.Parse(a.ID); err != nil {
		return fmt.Errorf("%w: id must be a UUID", ErrInvalidDeliveryAttempt)
	}
	if _, err := uuid.Parse(a.MessageID); err != nil {
		return fmt.Errorf("%w: message id must be a UUID", ErrInvalidDeliveryAttempt)
	}
	if a.AttemptNumber == 0 || a.AttemptNumber > math.MaxInt32 {
		return fmt.Errorf("%w: attempt number must fit a positive PostgreSQL INTEGER", ErrInvalidDeliveryAttempt)
	}
	if a.DispatchGeneration == 0 || a.DispatchGeneration > math.MaxInt64 {
		return fmt.Errorf("%w: dispatch generation must fit a positive PostgreSQL BIGINT", ErrInvalidDeliveryAttempt)
	}
	if err := ValidateProviderKey(a.ProviderKey); err != nil {
		return err
	}
	if a.StartedAt.IsZero() {
		return fmt.Errorf("%w: started time is required", ErrInvalidDeliveryAttempt)
	}
	return nil
}

// CompleteDeliveryAttempt uses STARTED as a compare-and-set fence. Only one
// worker or reconciler may persist the final observation for an attempt.
type CompleteDeliveryAttempt struct {
	AttemptID         string
	Status            DeliveryAttemptStatus
	FinishedAt        time.Time
	ProviderMessageID string
	Failure           *message.Failure
}

func (a CompleteDeliveryAttempt) Validate() error {
	if _, err := uuid.Parse(a.AttemptID); err != nil {
		return fmt.Errorf("%w: attempt id must be a UUID", ErrInvalidDeliveryAttempt)
	}
	if !a.Status.Valid() || a.Status == DeliveryAttemptStarted {
		return fmt.Errorf("%w: completion status %q is invalid", ErrInvalidDeliveryAttempt, a.Status)
	}
	if a.FinishedAt.IsZero() {
		return fmt.Errorf("%w: finished time is required", ErrInvalidDeliveryAttempt)
	}
	switch a.Status {
	case DeliveryAttemptProviderAccepted:
		if strings.TrimSpace(a.ProviderMessageID) == "" ||
			a.ProviderMessageID != strings.TrimSpace(a.ProviderMessageID) ||
			len(a.ProviderMessageID) > 512 {
			return fmt.Errorf("%w: provider message id must contain 1..512 bytes without surrounding whitespace", ErrInvalidDeliveryAttempt)
		}
		if a.Failure != nil {
			return fmt.Errorf("%w: accepted attempt cannot contain a failure", ErrInvalidDeliveryAttempt)
		}
	case DeliveryAttemptFailed:
		if a.ProviderMessageID != "" || a.Failure == nil {
			return fmt.Errorf("%w: failed attempt requires only failure information", ErrInvalidDeliveryAttempt)
		}
		if err := a.Failure.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidDeliveryAttempt, err)
		}
		if a.Failure.Category == message.FailureSubmissionUnknown {
			return fmt.Errorf("%w: ambiguous failure requires submission-unknown status", ErrInvalidDeliveryAttempt)
		}
	case DeliveryAttemptSubmissionUnknown:
		if a.ProviderMessageID != "" || a.Failure == nil {
			return fmt.Errorf("%w: submission-unknown attempt requires only failure information", ErrInvalidDeliveryAttempt)
		}
		if err := a.Failure.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidDeliveryAttempt, err)
		}
		if a.Failure.Category != message.FailureSubmissionUnknown {
			return fmt.Errorf("%w: submission-unknown status requires matching failure category", ErrInvalidDeliveryAttempt)
		}
	}
	return nil
}

func ValidateProviderKey(value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed != value || len(value) > 128 || strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("%w: provider key must contain 1..128 bytes without surrounding whitespace", ErrInvalidDeliveryAttempt)
	}
	return nil
}

type DeliveryAttemptRepository interface {
	CreateStarted(context.Context, StartedDeliveryAttempt) error
	Complete(context.Context, CompleteDeliveryAttempt) error
}
