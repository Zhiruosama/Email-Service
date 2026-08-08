package ports

import (
	"errors"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestMessageRecordValidateForCreate(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	aggregate, err := message.New(message.NewParams{
		ID:               "20000000-0000-4000-8000-000000000001",
		Now:              now,
		DispatchDeadline: now.Add(10 * time.Minute),
		MaxAttempts:      3,
	})
	if err != nil {
		t.Fatalf("create message: %v", err)
	}

	valid := MessageRecord{
		TenantID:            "10000000-0000-4000-8000-000000000001",
		IdempotencyKey:      "verification-001",
		Category:            EmailCategoryCritical,
		Priority:            9,
		DuplicateRiskPolicy: DuplicateRiskAvoidDuplicate,
		Message:             aggregate,
	}
	if err := valid.ValidateForCreate(); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*MessageRecord)
	}{
		{name: "tenant UUID", mutate: func(record *MessageRecord) { record.TenantID = "invalid" }},
		{name: "idempotency key", mutate: func(record *MessageRecord) { record.IdempotencyKey = " " }},
		{name: "category", mutate: func(record *MessageRecord) { record.Category = "UNKNOWN" }},
		{name: "priority", mutate: func(record *MessageRecord) { record.Priority = 10 }},
		{name: "risk policy", mutate: func(record *MessageRecord) { record.DuplicateRiskPolicy = "UNKNOWN" }},
		{name: "message", mutate: func(record *MessageRecord) { record.Message = nil }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := valid
			test.mutate(&record)
			if err := record.ValidateForCreate(); !errors.Is(err, ErrInvalidMessageRecord) {
				t.Fatalf("ValidateForCreate() error = %v, want ErrInvalidMessageRecord", err)
			}
		})
	}
}

func TestRepositoryValueEnums(t *testing.T) {
	if !EmailCategoryCritical.Valid() || EmailCategory("UNKNOWN").Valid() {
		t.Fatal("email category validity is incorrect")
	}
	if !DuplicateRiskAvoidDuplicate.Valid() || DuplicateRiskPolicy("UNKNOWN").Valid() {
		t.Fatal("duplicate risk policy validity is incorrect")
	}
}
