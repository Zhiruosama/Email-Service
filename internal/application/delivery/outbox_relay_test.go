package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

func TestOutboxRelayConfigValidate(t *testing.T) {
	t.Parallel()

	valid := relayTestConfig()
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*OutboxRelayConfig)
	}{
		{name: "instance", mutate: func(config *OutboxRelayConfig) { config.InstanceID = "bad/id" }},
		{name: "batch", mutate: func(config *OutboxRelayConfig) { config.BatchSize = 0 }},
		{name: "concurrency", mutate: func(config *OutboxRelayConfig) { config.PublishConcurrency = config.BatchSize + 1 }},
		{name: "lease", mutate: func(config *OutboxRelayConfig) { config.LeaseDuration = time.Millisecond }},
		{name: "timeout", mutate: func(config *OutboxRelayConfig) { config.PublishTimeout = config.LeaseDuration }},
		{name: "attempts", mutate: func(config *OutboxRelayConfig) { config.MaxAttempts = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := valid
			test.mutate(&config)
			if err := config.Validate(); !errors.Is(err, ErrInvalidOutboxRelayConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidOutboxRelayConfig", err)
			}
		})
	}
}

func TestFullJitterBackoffBounds(t *testing.T) {
	t.Parallel()

	backoff, err := NewFullJitterBackoff(time.Second, 5*time.Second)
	if err != nil {
		t.Fatalf("new backoff: %v", err)
	}
	for attempt, maximum := range map[uint32]time.Duration{
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 5 * time.Second,
		8: 5 * time.Second,
	} {
		for range 100 {
			delay := backoff.NextDelay(attempt)
			if delay < 0 || delay > maximum {
				t.Fatalf("attempt %d delay = %s, want 0..%s", attempt, delay, maximum)
			}
		}
	}
	if _, err := NewFullJitterBackoff(0, time.Second); !errors.Is(err, ErrInvalidOutboxRelayConfig) {
		t.Fatalf("invalid backoff error = %v", err)
	}
}

func TestClassifyPublishFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		publishErr error
		contextErr error
		code       string
		retryable  bool
	}{
		{name: "timeout", contextErr: context.DeadlineExceeded, code: publishTimeoutCode, retryable: true},
		{name: "canceled", contextErr: context.Canceled, code: publishCanceledCode, retryable: true},
		{
			name:       "typed",
			publishErr: ports.NewOutboxPublishError("UNROUTABLE", false, errors.New("detail")),
			code:       "UNROUTABLE",
		},
		{
			name:       "invalid typed",
			publishErr: ports.NewOutboxPublishError("unsafe response", false, nil),
			code:       publishInternalCode,
			retryable:  true,
		},
		{name: "raw", publishErr: errors.New("raw broker error"), code: publishInternalCode, retryable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := classifyPublishFailure(test.publishErr, test.contextErr)
			if got.code != test.code || got.retryable != test.retryable {
				t.Fatalf("classification = %#v, want code=%s retryable=%t", got, test.code, test.retryable)
			}
		})
	}
}

func TestValidateClaimedOutboxBatchRejectsMissingDatabaseTime(t *testing.T) {
	t.Parallel()

	batch := ports.OutboxClaimBatch{
		Events: []ports.LeasedOutboxEvent{{}},
	}
	if err := validateClaimedOutboxBatch(batch, "relay/token"); !errors.Is(err, ErrOutboxRelayInvariant) {
		t.Fatalf("batch error = %v, want ErrOutboxRelayInvariant", err)
	}
}

func TestNewOutboxRelayRejectsNilDependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		function func()
	}{
		{
			name: "transactor",
			function: func() {
				_, _ = NewOutboxRelay(nil, noCallPublisher{}, fixedRetryPolicy(0), relayTestConfig())
			},
		},
		{
			name: "publisher",
			function: func() {
				_, _ = NewOutboxRelay(noCallTransactor{}, nil, fixedRetryPolicy(0), relayTestConfig())
			},
		},
		{
			name: "retry policy",
			function: func() {
				_, _ = NewOutboxRelay(noCallTransactor{}, noCallPublisher{}, nil, relayTestConfig())
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Fatal("constructor did not panic")
				}
			}()
			test.function()
		})
	}
}

func relayTestConfig() OutboxRelayConfig {
	return OutboxRelayConfig{
		InstanceID:         "relay-test",
		BatchSize:          10,
		PublishConcurrency: 2,
		LeaseDuration:      30 * time.Second,
		PublishTimeout:     5 * time.Second,
		MaxAttempts:        3,
	}
}

type noCallPublisher struct{}

func (noCallPublisher) Publish(context.Context, ports.OutboxPublication) error {
	panic("publisher must not be called")
}

type fixedRetryPolicy time.Duration

func (p fixedRetryPolicy) NextDelay(uint32) time.Duration { return time.Duration(p) }
