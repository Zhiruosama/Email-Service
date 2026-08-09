package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestDispatchCommandAndWorkerConfigValidation(t *testing.T) {
	t.Parallel()
	command := DispatchCommand{
		EventID:            "48000000-0000-4000-8000-000000000001",
		TenantID:           "49000000-0000-4000-8000-000000000001",
		MessageID:          "4a000000-0000-4000-8000-000000000001",
		AggregateSequence:  2,
		DispatchGeneration: 1,
	}
	if err := command.Validate(); err != nil {
		t.Fatalf("valid command rejected: %v", err)
	}
	command.DispatchGeneration = 0
	if err := command.Validate(); !errors.Is(err, ErrInvalidDispatchCommand) {
		t.Fatalf("invalid command error = %v, want ErrInvalidDispatchCommand", err)
	}

	validConfig := DispatchWorkerConfig{
		ProviderTimeout: time.Second,
		FinalizeTimeout: time.Second,
	}
	if err := validConfig.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if _, err := NewDispatchWorker(
		noCallTransactor{},
		testEmailProvider{},
		constantDeliveryRetry(time.Second),
		validConfig,
	); err != nil {
		t.Fatalf("valid Worker rejected: %v", err)
	}
	validConfig.ProviderTimeout = 0
	if err := validConfig.Validate(); !errors.Is(err, ErrInvalidDispatchWorkerConfig) {
		t.Fatalf("invalid config error = %v, want ErrInvalidDispatchWorkerConfig", err)
	}
}

func TestDispatchWorkerMapsKnownProviderFailures(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 9, 13, 0, 0, 0, time.UTC)
	retryable := message.Failure{
		Category:  message.FailureProviderDown,
		Code:      "PROVIDER_UNAVAILABLE",
		Retryable: true,
	}
	permanent := message.Failure{
		Category:  message.FailureAuthentication,
		Code:      "INVALID_CREDENTIALS",
		Retryable: false,
	}

	tests := []struct {
		name            string
		maxAttempts     uint32
		failure         message.Failure
		wantAttempt     ports.DeliveryAttemptStatus
		wantDisposition DispatchDisposition
		wantMessage     message.Status
	}{
		{
			name:            "retry scheduled",
			maxAttempts:     3,
			failure:         retryable,
			wantAttempt:     ports.DeliveryAttemptFailed,
			wantDisposition: DispatchRetryScheduled,
			wantMessage:     message.StatusRetryScheduled,
		},
		{
			name:            "attempt limit dead letters",
			maxAttempts:     1,
			failure:         retryable,
			wantAttempt:     ports.DeliveryAttemptFailed,
			wantDisposition: DispatchDeadLettered,
			wantMessage:     message.StatusDeadLettered,
		},
		{
			name:            "known permanent failure",
			maxAttempts:     3,
			failure:         permanent,
			wantAttempt:     ports.DeliveryAttemptFailed,
			wantDisposition: DispatchPermanentlyFailed,
			wantMessage:     message.StatusPermanentlyFailed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			aggregate, err := message.New(message.NewParams{
				ID:               "4b000000-0000-4000-8000-000000000001",
				Now:              now,
				DispatchDeadline: now.Add(10 * time.Minute),
				MaxAttempts:      test.maxAttempts,
			})
			if err != nil {
				t.Fatalf("create message: %v", err)
			}
			aggregate.PullEvents()
			if err := aggregate.StartSending(1, now.Add(time.Second)); err != nil {
				t.Fatalf("start sending: %v", err)
			}
			aggregate.PullEvents()

			worker := &DispatchWorker{retry: constantDeliveryRetry(time.Minute)}
			attempt := ports.StartedDeliveryAttempt{
				ID:                 "4c000000-0000-4000-8000-000000000001",
				MessageID:          aggregate.ID(),
				AttemptNumber:      1,
				DispatchGeneration: 1,
				ProviderKey:        "fake",
				StartedAt:          now.Add(time.Second),
			}
			failure := test.failure
			completion, disposition, err := worker.applyProviderResult(
				aggregate,
				attempt,
				ports.ProviderResult{
					Outcome: ports.ProviderOutcomeFailed,
					Failure: &failure,
				},
				now.Add(2*time.Second),
			)
			if err != nil {
				t.Fatalf("apply provider result: %v", err)
			}
			if completion.Status != test.wantAttempt ||
				disposition != test.wantDisposition ||
				aggregate.Status() != test.wantMessage {
				t.Fatalf(
					"mapped result = attempt:%s disposition:%s message:%s",
					completion.Status,
					disposition,
					aggregate.Status(),
				)
			}
		})
	}
}

func TestDeliveryFullJitterBackoffBounds(t *testing.T) {
	t.Parallel()
	backoff, err := NewDeliveryFullJitterBackoff(100*time.Millisecond, time.Second)
	if err != nil {
		t.Fatalf("new delivery backoff: %v", err)
	}
	for range 100 {
		delay := backoff.NextDelay(50)
		if delay < time.Millisecond || delay > time.Second {
			t.Fatalf("delay %s outside [1ms, 1s]", delay)
		}
	}
	if _, err := NewDeliveryFullJitterBackoff(0, time.Second); !errors.Is(err, ErrInvalidDispatchWorkerConfig) {
		t.Fatalf("invalid backoff error = %v, want ErrInvalidDispatchWorkerConfig", err)
	}
}

func TestClassifyDispatchError(t *testing.T) {
	t.Parallel()
	if got := ClassifyDispatchError(ErrInvalidDispatchCommand); got != DispatchErrorPoison {
		t.Fatalf("invalid command class = %q, want POISON", got)
	}
	if got := ClassifyDispatchError(ports.ErrMessageRepository); got != DispatchErrorTransient {
		t.Fatalf("repository error class = %q, want TRANSIENT", got)
	}
	if got := ClassifyDispatchError(context.DeadlineExceeded); got != DispatchErrorTransient {
		t.Fatalf("deadline error class = %q, want TRANSIENT", got)
	}
}

type constantDeliveryRetry time.Duration

func (r constantDeliveryRetry) NextDelay(uint32) time.Duration { return time.Duration(r) }

type testEmailProvider struct{}

func (testEmailProvider) Key() string { return "fake" }

func (testEmailProvider) Submit(context.Context, ports.ProviderRequest) ports.ProviderResult {
	panic("Submit must not be called")
}
