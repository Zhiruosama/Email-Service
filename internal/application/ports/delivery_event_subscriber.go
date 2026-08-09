package ports

import (
	"context"
	"errors"
	"fmt"
)

var ErrInvalidDeliveryEventSubscriberError = errors.New("invalid delivery event subscriber error")

type EventAckDisposition string

const (
	EventAckAccepted     EventAckDisposition = "ACCEPTED"
	EventAckDuplicate    EventAckDisposition = "DUPLICATE"
	EventAckIgnoredStale EventAckDisposition = "IGNORED_STALE"
)

func (d EventAckDisposition) Valid() bool {
	switch d {
	case EventAckAccepted, EventAckDuplicate, EventAckIgnoredStale:
		return true
	default:
		return false
	}
}

// DeliveryEventSubscriber is a transport-neutral callback boundary. The
// subscriber receives only the sanitized journal record, never the encrypted
// submission payload or template variables.
type DeliveryEventSubscriber interface {
	Report(context.Context, PersistedDeliveryEvent) (EventAckDisposition, error)
}

// DeliveryEventSubscriberError carries a stable retry decision across the
// adapter/application boundary. The underlying cause is retained for internal
// telemetry but is deliberately not exposed through errors.Unwrap.
type DeliveryEventSubscriberError struct {
	Code      string
	Retryable bool
	cause     error
}

func NewDeliveryEventSubscriberError(
	code string,
	retryable bool,
	cause error,
) *DeliveryEventSubscriberError {
	return &DeliveryEventSubscriberError{Code: code, Retryable: retryable, cause: cause}
}

func (e *DeliveryEventSubscriberError) Error() string {
	if e == nil {
		return "delivery event subscriber failed"
	}
	return fmt.Sprintf("delivery event subscriber failed: %s", e.Code)
}

func (e *DeliveryEventSubscriberError) Cause() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *DeliveryEventSubscriberError) Validate() error {
	if e == nil || !validStableCode(e.Code) {
		return fmt.Errorf(
			"%w: code must be a stable 1..128 byte identifier",
			ErrInvalidDeliveryEventSubscriberError,
		)
	}
	return nil
}
