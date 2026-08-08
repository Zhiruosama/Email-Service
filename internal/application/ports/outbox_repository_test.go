package ports

import (
	"bytes"
	"errors"
	"testing"
)

func TestOutboxEventValidate(t *testing.T) {
	valid := OutboxEvent{
		ID:                 "60000000-0000-4000-8000-000000000001",
		AggregateType:      OutboxAggregateMailMessage,
		AggregateID:        "50000000-0000-4000-8000-000000000001",
		EventType:          "MESSAGE_DISPATCH_REQUESTED",
		AggregateSequence:  2,
		DispatchGeneration: 1,
		Payload:            []byte(`{"schema_version":1}`),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*OutboxEvent)
	}{
		{name: "event id", mutate: func(event *OutboxEvent) { event.ID = "invalid" }},
		{name: "aggregate type", mutate: func(event *OutboxEvent) { event.AggregateType = "" }},
		{name: "aggregate id", mutate: func(event *OutboxEvent) { event.AggregateID = "invalid" }},
		{name: "event type", mutate: func(event *OutboxEvent) { event.EventType = "" }},
		{name: "sequence", mutate: func(event *OutboxEvent) { event.AggregateSequence = 0 }},
		{name: "invalid JSON", mutate: func(event *OutboxEvent) { event.Payload = []byte(`{"`) }},
		{name: "non-object JSON", mutate: func(event *OutboxEvent) { event.Payload = []byte(`[]`) }},
		{name: "oversized JSON", mutate: func(event *OutboxEvent) {
			event.Payload = append([]byte(`{"value":"`), bytes.Repeat([]byte("x"), maxOutboxPayloadBytes)...)
			event.Payload = append(event.Payload, []byte(`"}`)...)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := valid
			test.mutate(&event)
			if err := event.Validate(); !errors.Is(err, ErrInvalidOutboxEvent) {
				t.Fatalf("Validate() error = %v, want ErrInvalidOutboxEvent", err)
			}
		})
	}
}
