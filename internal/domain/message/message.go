package message

import (
	"fmt"
	"strings"
	"time"
)

// NewParams contains the invariants required to create a new logical email.
type NewParams struct {
	ID               string
	Now              time.Time
	ScheduledAt      *time.Time
	DispatchDeadline time.Time
	MaxAttempts      uint32
}

// Snapshot is the persistence-facing representation used to rehydrate a
// Message without replaying historical domain events.
type Snapshot struct {
	ID                 string
	Status             Status
	ScheduledAt        *time.Time
	DispatchDeadline   time.Time
	NextAttemptAt      *time.Time
	AttemptCount       uint32
	MaxAttempts        uint32
	DispatchGeneration uint64
	ProviderAcceptedAt *time.Time
	ProviderMessageID  string
	LatestSequence     uint64
	Version            uint64
	AcceptedAt         time.Time
	UpdatedAt          time.Time
	LastFailure        *Failure
}

// Message is the aggregate root for one logical email. It is request-scoped;
// callers must not share one instance across goroutines.
type Message struct {
	id                 string
	status             Status
	scheduledAt        *time.Time
	dispatchDeadline   time.Time
	nextAttemptAt      *time.Time
	attemptCount       uint32
	maxAttempts        uint32
	dispatchGeneration uint64
	providerAcceptedAt *time.Time
	providerMessageID  string
	latestSequence     uint64
	version            uint64
	acceptedAt         time.Time
	updatedAt          time.Time
	lastFailure        *Failure
	pendingEvents      []Event
}

// New creates an accepted Message and immediately routes it to SCHEDULED or
// QUEUED. Immediate messages emit a dispatch-requested domain event.
func New(params NewParams) (*Message, error) {
	now := params.Now.UTC()
	deadline := params.DispatchDeadline.UTC()
	if strings.TrimSpace(params.ID) == "" {
		return nil, fmt.Errorf("%w: id is required", ErrInvalidMessage)
	}
	if params.Now.IsZero() || params.DispatchDeadline.IsZero() {
		return nil, fmt.Errorf("%w: now and dispatch deadline are required", ErrInvalidMessage)
	}
	if !deadline.After(now) {
		return nil, fmt.Errorf("%w: deadline must be after acceptance", ErrInvalidMessage)
	}
	if params.MaxAttempts == 0 {
		return nil, fmt.Errorf("%w: max attempts must be greater than zero", ErrInvalidMessage)
	}

	var scheduledAt *time.Time
	if params.ScheduledAt != nil {
		value := params.ScheduledAt.UTC()
		if !value.Before(deadline) {
			return nil, fmt.Errorf("%w: scheduled time must be before deadline", ErrInvalidMessage)
		}
		scheduledAt = &value
	}

	message := &Message{
		id:               params.ID,
		dispatchDeadline: deadline,
		maxAttempts:      params.MaxAttempts,
		acceptedAt:       now,
		updatedAt:        now,
		scheduledAt:      scheduledAt,
	}
	message.changeStatus(StatusAccepted, now, eventDetails{kind: EventMessageAccepted})

	if scheduledAt != nil && scheduledAt.After(now) {
		message.changeStatus(StatusScheduled, now, eventDetails{kind: EventStatusChanged})
		return message, nil
	}
	if err := message.Queue(now); err != nil {
		return nil, err
	}
	return message, nil
}

// Restore rehydrates an aggregate without producing new domain events.
func Restore(snapshot Snapshot) (*Message, error) {
	if err := validateSnapshot(snapshot); err != nil {
		return nil, err
	}
	message := &Message{
		id:                 snapshot.ID,
		status:             snapshot.Status,
		scheduledAt:        cloneTime(snapshot.ScheduledAt),
		dispatchDeadline:   snapshot.DispatchDeadline.UTC(),
		nextAttemptAt:      cloneTime(snapshot.NextAttemptAt),
		attemptCount:       snapshot.AttemptCount,
		maxAttempts:        snapshot.MaxAttempts,
		dispatchGeneration: snapshot.DispatchGeneration,
		providerAcceptedAt: cloneTime(snapshot.ProviderAcceptedAt),
		providerMessageID:  snapshot.ProviderMessageID,
		latestSequence:     snapshot.LatestSequence,
		version:            snapshot.Version,
		acceptedAt:         snapshot.AcceptedAt.UTC(),
		updatedAt:          snapshot.UpdatedAt.UTC(),
		lastFailure:        cloneFailure(snapshot.LastFailure),
	}
	return message, nil
}

func validateSnapshot(snapshot Snapshot) error {
	if strings.TrimSpace(snapshot.ID) == "" {
		return fmt.Errorf("%w: id is required", ErrInvalidMessage)
	}
	if !snapshot.Status.Valid() {
		return fmt.Errorf("%w: %q", ErrInvalidStatus, snapshot.Status)
	}
	if snapshot.AcceptedAt.IsZero() || snapshot.UpdatedAt.IsZero() || snapshot.DispatchDeadline.IsZero() {
		return fmt.Errorf("%w: persisted timestamps are required", ErrInvalidMessage)
	}
	if !snapshot.DispatchDeadline.After(snapshot.AcceptedAt) {
		return fmt.Errorf("%w: deadline must be after acceptance", ErrInvalidMessage)
	}
	if snapshot.UpdatedAt.Before(snapshot.AcceptedAt) {
		return fmt.Errorf("%w: updated time precedes acceptance", ErrInvalidMessage)
	}
	if snapshot.MaxAttempts == 0 || snapshot.AttemptCount > snapshot.MaxAttempts {
		return fmt.Errorf("%w: invalid attempt counters", ErrInvalidMessage)
	}
	if snapshot.ScheduledAt != nil && !snapshot.ScheduledAt.Before(snapshot.DispatchDeadline) {
		return fmt.Errorf("%w: scheduled time must be before deadline", ErrInvalidMessage)
	}
	if snapshot.NextAttemptAt != nil && !snapshot.NextAttemptAt.Before(snapshot.DispatchDeadline) {
		return fmt.Errorf("%w: retry time must be before deadline", ErrInvalidMessage)
	}
	if requiresDispatchGeneration(snapshot.Status) && snapshot.DispatchGeneration == 0 {
		return fmt.Errorf("%w: dispatch generation is required for active dispatch state", ErrInvalidMessage)
	}
	if requiresAttempt(snapshot.Status) && snapshot.AttemptCount == 0 {
		return fmt.Errorf("%w: dispatch state requires an attempt", ErrInvalidMessage)
	}
	if snapshot.Status == StatusRetryScheduled && snapshot.NextAttemptAt == nil {
		return fmt.Errorf("%w: retry-scheduled state requires next attempt time", ErrInvalidMessage)
	}
	if snapshot.LatestSequence == 0 {
		return fmt.Errorf("%w: latest sequence must be greater than zero", ErrInvalidMessage)
	}
	if snapshot.LastFailure != nil {
		if err := snapshot.LastFailure.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (m *Message) ID() string                     { return m.id }
func (m *Message) Status() Status                 { return m.status }
func (m *Message) AttemptCount() uint32           { return m.attemptCount }
func (m *Message) MaxAttempts() uint32            { return m.maxAttempts }
func (m *Message) DispatchGeneration() uint64     { return m.dispatchGeneration }
func (m *Message) LatestSequence() uint64         { return m.latestSequence }
func (m *Message) Version() uint64                { return m.version }
func (m *Message) DispatchDeadline() time.Time    { return m.dispatchDeadline }
func (m *Message) ScheduledAt() *time.Time        { return cloneTime(m.scheduledAt) }
func (m *Message) NextAttemptAt() *time.Time      { return cloneTime(m.nextAttemptAt) }
func (m *Message) ProviderAcceptedAt() *time.Time { return cloneTime(m.providerAcceptedAt) }

// Snapshot returns an isolated copy suitable for persistence.
func (m *Message) Snapshot() Snapshot {
	return Snapshot{
		ID:                 m.id,
		Status:             m.status,
		ScheduledAt:        cloneTime(m.scheduledAt),
		DispatchDeadline:   m.dispatchDeadline,
		NextAttemptAt:      cloneTime(m.nextAttemptAt),
		AttemptCount:       m.attemptCount,
		MaxAttempts:        m.maxAttempts,
		DispatchGeneration: m.dispatchGeneration,
		ProviderAcceptedAt: cloneTime(m.providerAcceptedAt),
		ProviderMessageID:  m.providerMessageID,
		LatestSequence:     m.latestSequence,
		Version:            m.version,
		AcceptedAt:         m.acceptedAt,
		UpdatedAt:          m.updatedAt,
		LastFailure:        cloneFailure(m.lastFailure),
	}
}

// PendingEvents returns a copy without clearing the aggregate event buffer.
func (m *Message) PendingEvents() []Event {
	events := make([]Event, len(m.pendingEvents))
	copy(events, m.pendingEvents)
	return events
}

// PullEvents returns and clears domain events after application code has
// persisted the Message and mapped selected events to Outbox records.
func (m *Message) PullEvents() []Event {
	events := m.PendingEvents()
	m.pendingEvents = nil
	return events
}

func (m *Message) changeStatus(to Status, occurredAt time.Time, details eventDetails) {
	from := m.status
	m.status = to
	m.updatedAt = occurredAt.UTC()
	m.latestSequence++
	m.pendingEvents = append(m.pendingEvents, Event{
		Kind:               details.kind,
		MessageID:          m.id,
		From:               from,
		To:                 to,
		OccurredAt:         occurredAt.UTC(),
		Sequence:           m.latestSequence,
		DispatchGeneration: m.dispatchGeneration,
		AttemptNumber:      m.attemptCount,
		ProviderMessageID:  details.providerMessageID,
		ReasonCode:         details.reasonCode,
		Failure:            details.failure,
	})
}

func (m *Message) requestDispatch(now time.Time) {
	m.pendingEvents = append(m.pendingEvents, Event{
		Kind:               EventDispatchRequested,
		MessageID:          m.id,
		From:               m.status,
		To:                 m.status,
		OccurredAt:         now.UTC(),
		Sequence:           m.latestSequence,
		DispatchGeneration: m.dispatchGeneration,
		AttemptNumber:      m.attemptCount,
	})
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := value.UTC()
	return &cloned
}

func requiresDispatchGeneration(status Status) bool {
	return oneOf(
		status,
		StatusQueued,
		StatusSending,
		StatusRetryScheduled,
		StatusSubmissionUnknown,
		StatusProviderAccepted,
		StatusDelivered,
		StatusBounced,
		StatusComplained,
		StatusPermanentlyFailed,
		StatusDeadLettered,
		StatusUnknownFinal,
	)
}

func requiresAttempt(status Status) bool {
	return oneOf(
		status,
		StatusSending,
		StatusRetryScheduled,
		StatusSubmissionUnknown,
		StatusProviderAccepted,
		StatusDelivered,
		StatusBounced,
		StatusComplained,
		StatusPermanentlyFailed,
		StatusDeadLettered,
		StatusUnknownFinal,
	)
}

func cloneFailure(value *Failure) *Failure {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
