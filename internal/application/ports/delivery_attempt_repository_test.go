package ports

import (
	"errors"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestDeliveryAttemptValidation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	started := StartedDeliveryAttempt{
		ID:                 "41000000-0000-4000-8000-000000000001",
		MessageID:          "42000000-0000-4000-8000-000000000001",
		AttemptNumber:      1,
		DispatchGeneration: 1,
		ProviderKey:        "fake",
		StartedAt:          now,
	}
	if err := started.Validate(); err != nil {
		t.Fatalf("valid started attempt rejected: %v", err)
	}

	accepted := CompleteDeliveryAttempt{
		AttemptID:         started.ID,
		Status:            DeliveryAttemptProviderAccepted,
		FinishedAt:        now.Add(time.Second),
		ProviderMessageID: "fake-message-1",
	}
	if err := accepted.Validate(); err != nil {
		t.Fatalf("valid accepted completion rejected: %v", err)
	}

	failure := message.Failure{
		Category:  message.FailureRateLimited,
		Code:      "RATE_LIMITED",
		Retryable: true,
	}
	failed := CompleteDeliveryAttempt{
		AttemptID:  started.ID,
		Status:     DeliveryAttemptFailed,
		FinishedAt: now.Add(time.Second),
		Failure:    &failure,
	}
	if err := failed.Validate(); err != nil {
		t.Fatalf("valid failed completion rejected: %v", err)
	}

	invalid := started
	invalid.AttemptNumber = 0
	if err := invalid.Validate(); !errors.Is(err, ErrInvalidDeliveryAttempt) {
		t.Fatalf("invalid started attempt error = %v, want ErrInvalidDeliveryAttempt", err)
	}

	unknownFailure := failure
	unknownFailure.Category = message.FailureSubmissionUnknown
	failed.Failure = &unknownFailure
	if err := failed.Validate(); !errors.Is(err, ErrInvalidDeliveryAttempt) {
		t.Fatalf("mismatched completion error = %v, want ErrInvalidDeliveryAttempt", err)
	}
}
