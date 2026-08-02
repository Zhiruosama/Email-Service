package message

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidMessage           = errors.New("invalid message")
	ErrInvalidStatus            = errors.New("invalid status")
	ErrInvalidTransition        = errors.New("invalid status transition")
	ErrDispatchDeadlineExceeded = errors.New("dispatch deadline exceeded")
	ErrDispatchDeadlineNotMet   = errors.New("dispatch deadline has not been reached")
	ErrStaleDispatchGeneration  = errors.New("stale dispatch generation")
	ErrAttemptLimitReached      = errors.New("delivery attempt limit reached")
	ErrFailureNotRetryable      = errors.New("failure is not retryable")
	ErrInvalidRetryTime         = errors.New("invalid retry time")
	ErrTooLateToCancel          = errors.New("message can no longer be canceled")
	ErrInvalidDeliveryFact      = errors.New("invalid delivery fact")
)

// TransitionError describes a rejected state transition and supports
// errors.Is(err, ErrInvalidTransition).
type TransitionError struct {
	From Status
	To   Status
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("%s: %s -> %s", ErrInvalidTransition, e.From, e.To)
}

func (e *TransitionError) Unwrap() error {
	return ErrInvalidTransition
}
