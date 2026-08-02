package message

import "time"

// EventKind identifies an in-memory domain event. Application code later maps
// selected events to persistent Outbox records in the same database transaction
// as the Message snapshot.
type EventKind string

const (
	EventMessageAccepted   EventKind = "MESSAGE_ACCEPTED"
	EventStatusChanged     EventKind = "MESSAGE_STATUS_CHANGED"
	EventDispatchRequested EventKind = "MESSAGE_DISPATCH_REQUESTED"
)

// Event is an infrastructure-independent fact emitted by the aggregate.
type Event struct {
	Kind               EventKind
	MessageID          string
	From               Status
	To                 Status
	OccurredAt         time.Time
	Sequence           uint64
	DispatchGeneration uint64
	AttemptNumber      uint32
	ProviderMessageID  string
	ReasonCode         string
	Failure            Failure
}

type eventDetails struct {
	kind              EventKind
	providerMessageID string
	reasonCode        string
	failure           Failure
}
