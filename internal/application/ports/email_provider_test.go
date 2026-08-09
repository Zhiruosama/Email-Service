package ports

import (
	"errors"
	"testing"

	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestProviderRequestAndResultValidation(t *testing.T) {
	t.Parallel()
	request := ProviderRequest{
		AttemptID:           "43000000-0000-4000-8000-000000000001",
		MessageID:           "44000000-0000-4000-8000-000000000001",
		TenantID:            "45000000-0000-4000-8000-000000000001",
		AttemptNumber:       1,
		DispatchGeneration:  1,
		Category:            EmailCategoryCritical,
		DuplicateRiskPolicy: DuplicateRiskAvoidDuplicate,
		Material: DeliveryMaterial{
			EnvelopeFrom: "sender@example.com",
			EnvelopeTo:   "recipient@example.com",
			MIMEMessage:  []byte("Subject: test\r\n\r\nbody"),
		},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}

	accepted := ProviderResult{
		Outcome:           ProviderOutcomeAccepted,
		ProviderMessageID: "provider-message-1",
	}
	if err := accepted.Validate(); err != nil {
		t.Fatalf("valid accepted result rejected: %v", err)
	}

	failure := message.Failure{
		Category:  message.FailureProviderDown,
		Code:      "PROVIDER_UNAVAILABLE",
		Retryable: true,
	}
	failed := ProviderResult{Outcome: ProviderOutcomeFailed, Failure: &failure}
	if err := failed.Validate(); err != nil {
		t.Fatalf("valid failed result rejected: %v", err)
	}

	invalid := failed
	invalid.Outcome = ProviderOutcomeSubmissionUnknown
	if err := invalid.Validate(); !errors.Is(err, ErrInvalidProviderResult) {
		t.Fatalf("mismatched result error = %v, want ErrInvalidProviderResult", err)
	}

	request.AttemptID = "invalid"
	if err := request.Validate(); !errors.Is(err, ErrInvalidProviderRequest) {
		t.Fatalf("invalid request error = %v, want ErrInvalidProviderRequest", err)
	}
}
