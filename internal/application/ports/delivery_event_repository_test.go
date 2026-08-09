package ports

import (
	"errors"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestDeliveryEventValidate(t *testing.T) {
	valid := DeliveryEvent{
		ID:             "c0000000-0000-4000-8000-000000000001",
		TenantID:       "c0000000-0000-4000-8000-000000000002",
		MessageID:      "c0000000-0000-4000-8000-000000000003",
		IdempotencyKey: "request-1",
		Status:         message.StatusProviderAccepted,
		Sequence:       4,
		AttemptNumber:  1,
		OccurredAt:     time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid event: %v", err)
	}
	tests := []func(*DeliveryEvent){
		func(e *DeliveryEvent) { e.ID = "event" },
		func(e *DeliveryEvent) { e.TenantID = "tenant" },
		func(e *DeliveryEvent) { e.MessageID = "message" },
		func(e *DeliveryEvent) { e.IdempotencyKey = "" },
		func(e *DeliveryEvent) { e.Status = "UNKNOWN" },
		func(e *DeliveryEvent) { e.Sequence = 0 },
		func(e *DeliveryEvent) { e.OccurredAt = time.Time{} },
		func(e *DeliveryEvent) { e.ProviderMessageID = " bad" },
		func(e *DeliveryEvent) { e.Failure = &message.Failure{Code: "PARTIAL"} },
	}
	for index, mutate := range tests {
		event := valid
		mutate(&event)
		if err := event.Validate(); !errors.Is(err, ErrInvalidDeliveryEvent) {
			t.Errorf("case %d error = %v, want ErrInvalidDeliveryEvent", index, err)
		}
	}
}
