package smtp

import (
	"context"
	"net/textproto"
	"strings"
	"testing"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestProviderClassifiesSMTPOutcomes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		err           error
		wantOutcome   ports.ProviderOutcome
		wantCategory  message.FailureCategory
		wantCode      string
		wantRetryable bool
	}{
		{
			name:        "accepted",
			wantOutcome: ports.ProviderOutcomeAccepted,
		},
		{
			name:          "temporary recipient pressure",
			err:           &ExchangeError{Phase: PhaseRecipient, StatusCode: 450},
			wantOutcome:   ports.ProviderOutcomeFailed,
			wantCategory:  message.FailureRateLimited,
			wantCode:      "SMTP_RECIPIENT_450",
			wantRetryable: true,
		},
		{
			name:          "authentication rejected",
			err:           &ExchangeError{Phase: PhaseAuth, StatusCode: 535},
			wantOutcome:   ports.ProviderOutcomeFailed,
			wantCategory:  message.FailureAuthentication,
			wantCode:      "SMTP_AUTH_535",
			wantRetryable: false,
		},
		{
			name:          "recipient rejected",
			err:           &ExchangeError{Phase: PhaseRecipient, StatusCode: 550},
			wantOutcome:   ports.ProviderOutcomeFailed,
			wantCategory:  message.FailureRecipientRejected,
			wantCode:      "SMTP_RECIPIENT_550",
			wantRetryable: false,
		},
		{
			name:          "content rejected after known final response",
			err:           &ExchangeError{Phase: PhaseDataCommit, StatusCode: 554},
			wantOutcome:   ports.ProviderOutcomeFailed,
			wantCategory:  message.FailureContentRejected,
			wantCode:      "SMTP_DATA_COMMIT_554",
			wantRetryable: false,
		},
		{
			name:          "timeout before commit",
			err:           &ExchangeError{Phase: PhaseDataWrite, TimedOut: true, Network: true},
			wantOutcome:   ports.ProviderOutcomeFailed,
			wantCategory:  message.FailureTimeoutBeforeSend,
			wantCode:      "SMTP_TIMEOUT_BEFORE_COMMIT",
			wantRetryable: true,
		},
		{
			name:          "final response lost",
			err:           &ExchangeError{Phase: PhaseDataCommit, Network: true},
			wantOutcome:   ports.ProviderOutcomeSubmissionUnknown,
			wantCategory:  message.FailureSubmissionUnknown,
			wantCode:      "SMTP_DATA_COMMIT_RESULT_UNKNOWN",
			wantRetryable: false,
		},
		{
			name:          "auth mechanism mismatch",
			err:           &ExchangeError{Phase: PhaseAuth, Protocol: true},
			wantOutcome:   ports.ProviderOutcomeFailed,
			wantCategory:  message.FailureAuthentication,
			wantCode:      "SMTP_AUTH_MECHANISM_UNAVAILABLE",
			wantRetryable: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &scriptedTransport{err: test.err}
			provider, err := newWithTransport(validTestConfig(), transport)
			if err != nil {
				t.Fatalf("new provider: %v", err)
			}
			request := validProviderRequest()
			result := provider.Submit(context.Background(), request)
			if result.Outcome != test.wantOutcome {
				t.Fatalf("outcome = %q, want %q", result.Outcome, test.wantOutcome)
			}
			if err := result.Validate(); err != nil {
				t.Fatalf("provider returned invalid result: %v", err)
			}
			if test.wantOutcome == ports.ProviderOutcomeAccepted {
				if result.ProviderMessageID != "smtp/"+request.AttemptID {
					t.Fatalf("provider message id = %q", result.ProviderMessageID)
				}
				return
			}
			if result.Failure.Category != test.wantCategory ||
				result.Failure.Code != test.wantCode ||
				result.Failure.Retryable != test.wantRetryable {
				t.Fatalf("failure = %#v", result.Failure)
			}
		})
	}
}

func TestProviderRejectsMismatchedEnvelopeWithoutTransport(t *testing.T) {
	t.Parallel()
	transport := &scriptedTransport{}
	provider, err := newWithTransport(validTestConfig(), transport)
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	request := validProviderRequest()
	request.Material.EnvelopeFrom = "attacker@example.com"
	result := provider.Submit(context.Background(), request)
	if transport.calls != 0 || result.Outcome != ports.ProviderOutcomeFailed ||
		result.Failure.Code != "SMTP_REQUEST_INVALID" {
		t.Fatalf("mismatched envelope result = %#v, calls = %d", result, transport.calls)
	}
}

func TestExchangeErrorDoesNotRetainRemoteResponseText(t *testing.T) {
	t.Parallel()
	secret := "recipient@example.com verification 123456"
	err := exchangeError(
		context.Background(),
		PhaseRecipient,
		&textproto.Error{Code: 550, Msg: secret},
	)
	if err.StatusCode != 550 {
		t.Fatalf("status = %d, want 550", err.StatusCode)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("exchange error exposed remote response")
	}
}

type scriptedTransport struct {
	err   error
	calls int
}

func (t *scriptedTransport) Deliver(context.Context, ports.DeliveryMaterial) error {
	t.calls++
	return t.err
}

func validProviderRequest() ports.ProviderRequest {
	return ports.ProviderRequest{
		AttemptID:           "cc000000-0000-4000-8000-000000000001",
		MessageID:           "cc000000-0000-4000-8000-000000000002",
		TenantID:            "cc000000-0000-4000-8000-000000000003",
		AttemptNumber:       1,
		DispatchGeneration:  1,
		Category:            ports.EmailCategoryCritical,
		DuplicateRiskPolicy: ports.DuplicateRiskAvoidDuplicate,
		Material: ports.DeliveryMaterial{
			EnvelopeFrom: "sender@example.com",
			EnvelopeTo:   "recipient@example.com",
			MIMEMessage:  []byte("Subject: test\r\n\r\nbody"),
		},
	}
}
