package delivery

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestMapMessageEventsUsesSafeAllowlist(t *testing.T) {
	record := deliveryTestRecord(t, "80000000-0000-4000-8000-000000000001")
	events, err := mapMessageEvents(record, record.Message.PendingEvents())
	if err != nil {
		t.Fatalf("map immediate events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("mapped event count = %d, want 3", len(events))
	}

	kinds := make(map[string]bool, len(events))
	for _, event := range events {
		kinds[event.EventType] = true
		var envelope messageEventEnvelope
		if err := json.Unmarshal(event.Payload, &envelope); err != nil {
			t.Fatalf("decode event payload: %v", err)
		}
		if envelope.SchemaVersion != 1 ||
			envelope.TenantID != record.TenantID ||
			envelope.MessageID != record.Message.ID() {
			t.Fatalf("unexpected envelope: %#v", envelope)
		}
		payload := strings.ToLower(string(event.Payload))
		for _, forbidden := range []string{
			"recipient_email",
			"template_variables",
			"verification_code",
			"idempotency_key",
		} {
			if strings.Contains(payload, forbidden) {
				t.Fatalf("payload contains forbidden field %q: %s", forbidden, payload)
			}
		}
	}
	for _, kind := range []string{
		"MESSAGE_ACCEPTED",
		"MESSAGE_STATUS_CHANGED",
		"MESSAGE_DISPATCH_REQUESTED",
	} {
		if !kinds[kind] {
			t.Fatalf("event kind %q was not mapped", kind)
		}
	}
}

func TestMapMessageEventIncludesSanitizedFailure(t *testing.T) {
	record := deliveryTestRecord(t, "80000000-0000-4000-8000-000000000002")
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	if err := record.Message.StartSending(1, now.Add(time.Second)); err != nil {
		t.Fatalf("start sending: %v", err)
	}
	failure := message.Failure{
		Category:  message.FailureRateLimited,
		Code:      "PROVIDER_RATE_LIMITED",
		Retryable: true,
	}
	if err := record.Message.ScheduleRetry(
		failure,
		now.Add(3*time.Minute),
		now.Add(2*time.Second),
	); err != nil {
		t.Fatalf("schedule retry: %v", err)
	}
	pending := record.Message.PendingEvents()
	mapped, err := mapMessageEvent(record, pending[len(pending)-1])
	if err != nil {
		t.Fatalf("map retry event: %v", err)
	}
	var envelope messageEventEnvelope
	if err := json.Unmarshal(mapped.Payload, &envelope); err != nil {
		t.Fatalf("decode retry payload: %v", err)
	}
	if envelope.Failure == nil ||
		envelope.Failure.Category != "RATE_LIMITED" ||
		envelope.Failure.Code != "PROVIDER_RATE_LIMITED" ||
		!envelope.Failure.Retryable {
		t.Fatalf("unexpected failure envelope: %#v", envelope.Failure)
	}
}

func TestMapMessageEventsRejectsInvalidInput(t *testing.T) {
	record := deliveryTestRecord(t, "80000000-0000-4000-8000-000000000003")
	if _, err := mapMessageEvents(record, nil); !errors.Is(err, ErrNoPendingMessageEvents) {
		t.Fatalf("empty event error = %v, want ErrNoPendingMessageEvents", err)
	}

	base := record.Message.PendingEvents()[0]
	tests := []struct {
		name   string
		mutate func(*message.Event)
	}{
		{name: "aggregate", mutate: func(event *message.Event) { event.MessageID = "80000000-0000-4000-8000-000000000099" }},
		{name: "time", mutate: func(event *message.Event) { event.OccurredAt = time.Time{} }},
		{name: "sequence", mutate: func(event *message.Event) { event.Sequence = 0 }},
		{name: "kind", mutate: func(event *message.Event) { event.Kind = "UNKNOWN" }},
		{name: "partial failure", mutate: func(event *message.Event) { event.Failure.Code = "PARTIAL" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := base
			test.mutate(&event)
			if _, err := mapMessageEvent(record, event); !errors.Is(err, ErrMessageEventMapping) {
				t.Fatalf("map error = %v, want ErrMessageEventMapping", err)
			}
		})
	}
}

func TestNewReliableMessageStoreRejectsNilTransactor(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewReliableMessageStore(nil) did not panic")
		}
	}()
	NewReliableMessageStore(nil)
}

func deliveryTestRecord(t *testing.T, messageID string) ports.MessageRecord {
	t.Helper()
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	aggregate, err := message.New(message.NewParams{
		ID:               messageID,
		Now:              now,
		DispatchDeadline: now.Add(10 * time.Minute),
		MaxAttempts:      3,
	})
	if err != nil {
		t.Fatalf("create message: %v", err)
	}
	return ports.MessageRecord{
		TenantID:            "81000000-0000-4000-8000-000000000001",
		IdempotencyKey:      "delivery-test",
		PayloadFingerprint:  [32]byte{1},
		Category:            ports.EmailCategoryCritical,
		Priority:            9,
		DuplicateRiskPolicy: ports.DuplicateRiskAvoidDuplicate,
		Message:             aggregate,
	}
}
