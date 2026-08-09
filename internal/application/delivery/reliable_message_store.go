// Package delivery coordinates domain changes and reliable persistence.
package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"github.com/google/uuid"
)

var (
	ErrNoPendingMessageEvents = errors.New("message has no pending events")
	ErrMessageEventMapping    = errors.New("message event mapping failure")
)

type ReliableMessageStore struct {
	transactor ports.Transactor
}

var deliveryEventNamespace = uuid.MustParse("31ac7d78-5960-4bd4-b6ce-3fbeb3a7d66f")

type mappedMessageEvents struct {
	Outbox   []ports.OutboxEvent
	Delivery []ports.DeliveryEvent
}

func NewReliableMessageStore(transactor ports.Transactor) *ReliableMessageStore {
	if transactor == nil {
		panic("delivery: nil transactor")
	}
	return &ReliableMessageStore{transactor: transactor}
}

func (s *ReliableMessageStore) Create(
	ctx context.Context,
	record ports.MessageRecord,
) (ports.CreateMessageResult, error) {
	if err := record.ValidateForCreate(); err != nil {
		return ports.CreateMessageResult{}, err
	}
	mapped, err := mapAllMessageEvents(record, record.Message.PendingEvents())
	if err != nil {
		return ports.CreateMessageResult{}, err
	}

	var result ports.CreateMessageResult
	err = s.transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
		created, createErr := unit.Messages().Create(ctx, record)
		if createErr != nil {
			return createErr
		}
		result = created
		if created.Disposition == ports.CreateDispositionDuplicate {
			return nil
		}
		if appendErr := unit.DeliveryEvents().Append(ctx, mapped.Delivery); appendErr != nil {
			return appendErr
		}
		return unit.Outbox().Append(ctx, mapped.Outbox)
	})
	if err != nil {
		return ports.CreateMessageResult{}, err
	}
	if result.Disposition == ports.CreateDispositionCreated {
		record.Message.PullEvents()
	}
	return result, nil
}

func (s *ReliableMessageStore) Save(
	ctx context.Context,
	record ports.MessageRecord,
) (uint64, error) {
	if err := record.Validate(); err != nil {
		return 0, err
	}
	mapped, err := mapAllMessageEvents(record, record.Message.PendingEvents())
	if err != nil {
		return 0, err
	}

	var persistedVersion uint64
	err = s.transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
		version, saveErr := unit.Messages().Save(ctx, record.Message)
		if saveErr != nil {
			return saveErr
		}
		persistedVersion = version
		if appendErr := unit.DeliveryEvents().Append(ctx, mapped.Delivery); appendErr != nil {
			return appendErr
		}
		return unit.Outbox().Append(ctx, mapped.Outbox)
	})
	if err != nil {
		return 0, err
	}
	record.Message.PullEvents()
	return persistedVersion, nil
}

func mapMessageEvents(
	record ports.MessageRecord,
	events []message.Event,
) ([]ports.OutboxEvent, error) {
	mapped, err := mapAllMessageEvents(record, events)
	return mapped.Outbox, err
}

func mapAllMessageEvents(
	record ports.MessageRecord,
	events []message.Event,
) (mappedMessageEvents, error) {
	if len(events) == 0 {
		return mappedMessageEvents{}, ErrNoPendingMessageEvents
	}
	mapped := mappedMessageEvents{
		Outbox:   make([]ports.OutboxEvent, 0, len(events)),
		Delivery: make([]ports.DeliveryEvent, 0, len(events)),
	}
	for _, event := range events {
		outboxEvent, err := mapMessageEvent(record, event)
		if err != nil {
			return mappedMessageEvents{}, err
		}
		mapped.Outbox = append(mapped.Outbox, outboxEvent)
		if event.Kind == message.EventMessageAccepted || event.Kind == message.EventStatusChanged {
			deliveryEvent := ports.DeliveryEvent{
				ID:                outboxEvent.ID,
				TenantID:          record.TenantID,
				MessageID:         event.MessageID,
				IdempotencyKey:    record.IdempotencyKey,
				Status:            event.To,
				Sequence:          event.Sequence,
				AttemptNumber:     event.AttemptNumber,
				ProviderMessageID: event.ProviderMessageID,
				OccurredAt:        event.OccurredAt.UTC(),
			}
			if event.Failure.Category.Valid() {
				failure := event.Failure
				deliveryEvent.Failure = &failure
			}
			if err := deliveryEvent.Validate(); err != nil {
				return mappedMessageEvents{}, fmt.Errorf("%w: map delivery event: %v", ErrMessageEventMapping, err)
			}
			mapped.Delivery = append(mapped.Delivery, deliveryEvent)
		}
	}
	return mapped, nil
}

func mapMessageEvent(
	record ports.MessageRecord,
	event message.Event,
) (ports.OutboxEvent, error) {
	if event.MessageID != record.Message.ID() {
		return ports.OutboxEvent{}, fmt.Errorf("%w: aggregate id does not match event", ErrMessageEventMapping)
	}
	if event.OccurredAt.IsZero() || event.Sequence == 0 {
		return ports.OutboxEvent{}, fmt.Errorf("%w: event time and sequence are required", ErrMessageEventMapping)
	}
	if !knownMessageEventKind(event.Kind) {
		return ports.OutboxEvent{}, fmt.Errorf("%w: unknown event type %q", ErrMessageEventMapping, event.Kind)
	}

	envelope := messageEventEnvelope{
		SchemaVersion:      1,
		TenantID:           record.TenantID,
		MessageID:          event.MessageID,
		EventType:          string(event.Kind),
		From:               string(event.From),
		To:                 string(event.To),
		OccurredAt:         event.OccurredAt.UTC(),
		Sequence:           event.Sequence,
		DispatchGeneration: event.DispatchGeneration,
		AttemptNumber:      event.AttemptNumber,
		ProviderMessageID:  event.ProviderMessageID,
		ReasonCode:         event.ReasonCode,
	}
	if event.Failure.Category.Valid() {
		envelope.Failure = &messageFailureEnvelope{
			Category:  string(event.Failure.Category),
			Code:      event.Failure.Code,
			Retryable: event.Failure.Retryable,
		}
	} else if event.Failure.Code != "" || event.Failure.Retryable {
		return ports.OutboxEvent{}, fmt.Errorf("%w: partial failure information", ErrMessageEventMapping)
	}

	payload, err := json.Marshal(envelope)
	if err != nil {
		return ports.OutboxEvent{}, fmt.Errorf("%w: encode payload", ErrMessageEventMapping)
	}
	outboxEvent := ports.OutboxEvent{
		ID:                 stableMessageEventID(event),
		AggregateType:      ports.OutboxAggregateMailMessage,
		AggregateID:        event.MessageID,
		EventType:          string(event.Kind),
		AggregateSequence:  event.Sequence,
		DispatchGeneration: event.DispatchGeneration,
		Payload:            payload,
	}
	if err := outboxEvent.Validate(); err != nil {
		return ports.OutboxEvent{}, fmt.Errorf("%w: %v", ErrMessageEventMapping, err)
	}
	return outboxEvent, nil
}

func stableMessageEventID(event message.Event) string {
	identity := fmt.Sprintf(
		"mail-message/%s/%s/%d/%d",
		event.MessageID,
		event.Kind,
		event.Sequence,
		event.DispatchGeneration,
	)
	return uuid.NewSHA1(deliveryEventNamespace, []byte(identity)).String()
}

func knownMessageEventKind(kind message.EventKind) bool {
	switch kind {
	case message.EventMessageAccepted,
		message.EventStatusChanged,
		message.EventDispatchRequested:
		return true
	default:
		return false
	}
}

type messageEventEnvelope struct {
	SchemaVersion      uint32                  `json:"schema_version"`
	TenantID           string                  `json:"tenant_id"`
	MessageID          string                  `json:"message_id"`
	EventType          string                  `json:"event_type"`
	From               string                  `json:"from,omitempty"`
	To                 string                  `json:"to"`
	OccurredAt         time.Time               `json:"occurred_at"`
	Sequence           uint64                  `json:"sequence"`
	DispatchGeneration uint64                  `json:"dispatch_generation"`
	AttemptNumber      uint32                  `json:"attempt_number"`
	ProviderMessageID  string                  `json:"provider_message_id,omitempty"`
	ReasonCode         string                  `json:"reason_code,omitempty"`
	Failure            *messageFailureEnvelope `json:"failure,omitempty"`
}

type messageFailureEnvelope struct {
	Category  string `json:"category"`
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
}
