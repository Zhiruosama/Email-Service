package message

import (
	"fmt"
	"strings"
	"time"
)

// Queue moves an accepted, scheduled, or retry-scheduled Message into the
// dispatch queue and emits a new dispatch generation.
func (m *Message) Queue(now time.Time) error {
	now = now.UTC()
	if !now.Before(m.dispatchDeadline) {
		return ErrDispatchDeadlineExceeded
	}
	if !canTransition(m.status, StatusQueued) {
		return &TransitionError{From: m.status, To: StatusQueued}
	}

	m.dispatchGeneration++
	m.nextAttemptAt = nil
	m.changeStatus(StatusQueued, now, eventDetails{kind: EventStatusChanged})
	m.requestDispatch(now)
	return nil
}

// StartSending validates an MQ dispatch command and starts one provider
// attempt. Allocating the attempt before external I/O makes crashed attempts
// visible to the later reconciler.
func (m *Message) StartSending(generation uint64, now time.Time) error {
	now = now.UTC()
	if m.status != StatusQueued {
		return &TransitionError{From: m.status, To: StatusSending}
	}
	if generation != m.dispatchGeneration {
		return fmt.Errorf("%w: got %d, current %d", ErrStaleDispatchGeneration, generation, m.dispatchGeneration)
	}
	if !now.Before(m.dispatchDeadline) {
		return ErrDispatchDeadlineExceeded
	}
	if m.attemptCount >= m.maxAttempts {
		return ErrAttemptLimitReached
	}

	m.attemptCount++
	m.changeStatus(StatusSending, now, eventDetails{kind: EventStatusChanged})
	return nil
}

// ScheduleRetry records a retryable failure and a future business retry time.
// The Scheduler, not RabbitMQ, later moves the Message back to QUEUED.
func (m *Message) ScheduleRetry(failure Failure, nextAttemptAt, now time.Time) error {
	now = now.UTC()
	nextAttemptAt = nextAttemptAt.UTC()
	if !oneOf(m.status, StatusSending, StatusSubmissionUnknown) {
		return &TransitionError{From: m.status, To: StatusRetryScheduled}
	}
	if err := failure.validate(); err != nil {
		return err
	}
	if !failure.Retryable {
		return ErrFailureNotRetryable
	}
	if m.attemptCount >= m.maxAttempts {
		return ErrAttemptLimitReached
	}
	if !nextAttemptAt.After(now) || !nextAttemptAt.Before(m.dispatchDeadline) {
		return ErrInvalidRetryTime
	}

	m.nextAttemptAt = &nextAttemptAt
	m.lastFailure = cloneFailure(&failure)
	m.changeStatus(StatusRetryScheduled, now, eventDetails{kind: EventStatusChanged, failure: failure})
	return nil
}

// MarkSubmissionUnknown records an ambiguous provider submission result.
func (m *Message) MarkSubmissionUnknown(failure Failure, now time.Time) error {
	if m.status != StatusSending {
		return &TransitionError{From: m.status, To: StatusSubmissionUnknown}
	}
	if err := failure.validate(); err != nil {
		return err
	}
	if failure.Category != FailureSubmissionUnknown {
		return fmt.Errorf("%w: submission-unknown transition requires matching failure category", ErrInvalidMessage)
	}

	m.lastFailure = cloneFailure(&failure)
	m.changeStatus(StatusSubmissionUnknown, now, eventDetails{kind: EventStatusChanged, failure: failure})
	return nil
}

// MarkPermanentlyFailed records a non-recoverable dispatch failure.
func (m *Message) MarkPermanentlyFailed(failure Failure, now time.Time) error {
	if !oneOf(m.status, StatusSending, StatusSubmissionUnknown) {
		return &TransitionError{From: m.status, To: StatusPermanentlyFailed}
	}
	if err := failure.validate(); err != nil {
		return err
	}

	m.lastFailure = cloneFailure(&failure)
	m.nextAttemptAt = nil
	m.changeStatus(StatusPermanentlyFailed, now, eventDetails{kind: EventStatusChanged, failure: failure})
	return nil
}

// MarkDeadLettered terminates a message whose safe retry policy is exhausted.
func (m *Message) MarkDeadLettered(failure Failure, now time.Time) error {
	if !oneOf(m.status, StatusSending, StatusRetryScheduled) {
		return &TransitionError{From: m.status, To: StatusDeadLettered}
	}
	if err := failure.validate(); err != nil {
		return err
	}

	m.lastFailure = cloneFailure(&failure)
	m.nextAttemptAt = nil
	m.changeStatus(StatusDeadLettered, now, eventDetails{kind: EventStatusChanged, failure: failure})
	return nil
}

// MarkUnknownFinal terminates an ambiguous result that cannot be reconciled.
func (m *Message) MarkUnknownFinal(failure Failure, now time.Time) error {
	if m.status != StatusSubmissionUnknown {
		return &TransitionError{From: m.status, To: StatusUnknownFinal}
	}
	if err := failure.validate(); err != nil {
		return err
	}

	m.lastFailure = cloneFailure(&failure)
	m.changeStatus(StatusUnknownFinal, now, eventDetails{kind: EventStatusChanged, failure: failure})
	return nil
}

// Cancel is idempotent. It returns changed=false for an already canceled
// Message and ErrTooLateToCancel after provider execution has started.
func (m *Message) Cancel(reasonCode string, now time.Time) (bool, error) {
	if m.status == StatusCanceled {
		return false, nil
	}
	if !oneOf(m.status, StatusAccepted, StatusScheduled, StatusQueued, StatusRetryScheduled) {
		return false, ErrTooLateToCancel
	}
	if len(reasonCode) > 128 || strings.ContainsAny(reasonCode, "\r\n") {
		return false, fmt.Errorf("%w: invalid cancel reason code", ErrInvalidMessage)
	}

	m.nextAttemptAt = nil
	m.changeStatus(StatusCanceled, now, eventDetails{kind: EventStatusChanged, reasonCode: reasonCode})
	return true, nil
}

// Expire is idempotent and only succeeds at or after the dispatch deadline.
func (m *Message) Expire(now time.Time) (bool, error) {
	now = now.UTC()
	if m.status == StatusExpired {
		return false, nil
	}
	if now.Before(m.dispatchDeadline) {
		return false, ErrDispatchDeadlineNotMet
	}
	if !oneOf(m.status, StatusAccepted, StatusScheduled, StatusQueued, StatusRetryScheduled) {
		return false, &TransitionError{From: m.status, To: StatusExpired}
	}

	m.nextAttemptAt = nil
	m.changeStatus(StatusExpired, now, eventDetails{kind: EventStatusChanged})
	return true, nil
}

// DeliveryFactKind is a normalized provider observation.
type DeliveryFactKind string

const (
	FactProviderAccepted DeliveryFactKind = "PROVIDER_ACCEPTED"
	FactDelivered        DeliveryFactKind = "DELIVERED"
	FactBounced          DeliveryFactKind = "BOUNCED"
	FactComplained       DeliveryFactKind = "COMPLAINED"
)

// DeliveryFact represents an already-observed provider fact, not a command.
type DeliveryFact struct {
	Kind              DeliveryFactKind
	OccurredAt        time.Time
	ProviderMessageID string
}

type ApplyResult string

const (
	ApplyResultApplied           ApplyResult = "APPLIED"
	ApplyResultDuplicate         ApplyResult = "DUPLICATE"
	ApplyResultIgnoredRegression ApplyResult = "IGNORED_REGRESSION"
)

// ApplyDeliveryFact applies duplicate and out-of-order provider observations
// without allowing the aggregate state to move backward.
func (m *Message) ApplyDeliveryFact(fact DeliveryFact) (ApplyResult, error) {
	if fact.OccurredAt.IsZero() {
		return "", fmt.Errorf("%w: occurred_at is required", ErrInvalidDeliveryFact)
	}
	target, ok := deliveryFactTarget(fact.Kind)
	if !ok {
		return "", fmt.Errorf("%w: kind %q", ErrInvalidDeliveryFact, fact.Kind)
	}
	if m.status == target {
		return ApplyResultDuplicate, nil
	}
	if observationWouldRegress(m.status, target) {
		return ApplyResultIgnoredRegression, nil
	}
	if !canTransition(m.status, target) {
		return "", &TransitionError{From: m.status, To: target}
	}

	if fact.ProviderMessageID != "" && m.providerMessageID == "" {
		m.providerMessageID = fact.ProviderMessageID
	}
	if target == StatusProviderAccepted && m.providerAcceptedAt == nil {
		value := fact.OccurredAt.UTC()
		m.providerAcceptedAt = &value
	}
	m.lastFailure = nil
	m.nextAttemptAt = nil
	m.changeStatus(target, fact.OccurredAt, eventDetails{
		kind:              EventStatusChanged,
		providerMessageID: fact.ProviderMessageID,
	})
	return ApplyResultApplied, nil
}

func deliveryFactTarget(kind DeliveryFactKind) (Status, bool) {
	switch kind {
	case FactProviderAccepted:
		return StatusProviderAccepted, true
	case FactDelivered:
		return StatusDelivered, true
	case FactBounced:
		return StatusBounced, true
	case FactComplained:
		return StatusComplained, true
	default:
		return "", false
	}
}

func observationWouldRegress(current, target Status) bool {
	switch current {
	case StatusProviderAccepted:
		return target == StatusProviderAccepted
	case StatusDelivered:
		return oneOf(target, StatusProviderAccepted, StatusDelivered, StatusBounced)
	case StatusBounced:
		return oneOf(target, StatusProviderAccepted, StatusDelivered, StatusBounced, StatusComplained)
	case StatusComplained:
		return true
	default:
		return false
	}
}
