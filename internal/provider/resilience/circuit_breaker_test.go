package resilience

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestCircuitOpensAtFailureThresholdAndRejectsWithoutCallingProvider(t *testing.T) {
	t.Parallel()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	provider := &providerStub{result: failedResult(message.FailureNetwork, "TEST_NETWORK", true)}
	config := guardConfig(10, 100, 100)
	config.Circuit = CircuitConfig{FailureThreshold: 3, OpenDuration: 10 * time.Second}
	guard, err := newWithClock(provider, config, clock.Now)
	if err != nil {
		t.Fatalf("newWithClock() error = %v", err)
	}

	for range 3 {
		assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
			message.FailureNetwork, "TEST_NETWORK")
	}
	if state := guard.CircuitState(); state != CircuitOpen {
		t.Fatalf("CircuitState() = %q, want OPEN", state)
	}
	assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
		message.FailureProviderDown, circuitOpenCode)
	if calls := provider.calls.Load(); calls != 3 {
		t.Fatalf("provider calls = %d, want 3", calls)
	}
}

func TestCircuitAuthenticationFailureOpensImmediately(t *testing.T) {
	t.Parallel()
	provider := &providerStub{result: failedResult(message.FailureAuthentication, "TEST_AUTH", false)}
	config := guardConfig(2, 100, 100)
	config.Circuit.FailureThreshold = 100
	guard, err := New(provider, config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result := guard.Submit(context.Background(), ports.ProviderRequest{})
	if result.Failure == nil || result.Failure.Retryable {
		t.Fatalf("authentication result = %#v, want original permanent failure", result)
	}
	if state := guard.CircuitState(); state != CircuitOpen {
		t.Fatalf("CircuitState() = %q, want OPEN", state)
	}
	assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
		message.FailureProviderDown, circuitOpenCode)
	if calls := provider.calls.Load(); calls != 1 {
		t.Fatalf("provider calls = %d, want 1", calls)
	}
}

func TestCircuitHalfOpenProbeSuccessClosesCircuit(t *testing.T) {
	t.Parallel()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	provider := newMutableProvider(failedResult(message.FailureProviderDown, "TEST_DOWN", true))
	config := guardConfig(4, 100, 100)
	config.Circuit = CircuitConfig{FailureThreshold: 1, OpenDuration: 10 * time.Second}
	guard, err := newWithClock(provider, config, clock.Now)
	if err != nil {
		t.Fatalf("newWithClock() error = %v", err)
	}

	guard.Submit(context.Background(), ports.ProviderRequest{})
	clock.Advance(10 * time.Second)
	provider.SetResult(acceptedResult("probe-accepted"))
	if result := guard.Submit(context.Background(), ports.ProviderRequest{}); result.Outcome != ports.ProviderOutcomeAccepted {
		t.Fatalf("half-open probe result = %#v", result)
	}
	if state := guard.CircuitState(); state != CircuitClosed {
		t.Fatalf("CircuitState() = %q, want CLOSED", state)
	}
	if result := guard.Submit(context.Background(), ports.ProviderRequest{}); result.Outcome != ports.ProviderOutcomeAccepted {
		t.Fatalf("closed result = %#v", result)
	}
	if calls := provider.CallCount(); calls != 3 {
		t.Fatalf("provider calls = %d, want 3", calls)
	}
}

func TestCircuitAllowsOnlyOneHalfOpenProbe(t *testing.T) {
	t.Parallel()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	provider := newMutableProvider(failedResult(message.FailureNetwork, "TEST_NETWORK", true))
	config := guardConfig(4, 100, 100)
	config.Circuit = CircuitConfig{FailureThreshold: 1, OpenDuration: time.Second}
	guard, err := newWithClock(provider, config, clock.Now)
	if err != nil {
		t.Fatalf("newWithClock() error = %v", err)
	}
	guard.Submit(context.Background(), ports.ProviderRequest{})

	clock.Advance(time.Second)
	provider.BlockWithResult(acceptedResult("probe-accepted"))
	probeResult := make(chan ports.ProviderResult, 1)
	go func() { probeResult <- guard.Submit(context.Background(), ports.ProviderRequest{}) }()
	provider.WaitForEntry(t)
	if state := guard.CircuitState(); state != CircuitHalfOpen {
		t.Fatalf("CircuitState() = %q, want HALF_OPEN", state)
	}
	assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
		message.FailureProviderDown, circuitOpenCode)
	if calls := provider.CallCount(); calls != 2 {
		t.Fatalf("provider calls during probe = %d, want 2", calls)
	}
	provider.Release()
	if result := <-probeResult; result.Outcome != ports.ProviderOutcomeAccepted {
		t.Fatalf("probe result = %#v", result)
	}
	if state := guard.CircuitState(); state != CircuitClosed {
		t.Fatalf("CircuitState() after probe = %q, want CLOSED", state)
	}
}

func TestCircuitHalfOpenFailureRestartsOpenDuration(t *testing.T) {
	t.Parallel()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	provider := &providerStub{result: failedResult(message.FailureProviderDown, "TEST_DOWN", true)}
	config := guardConfig(4, 100, 100)
	config.Circuit = CircuitConfig{FailureThreshold: 1, OpenDuration: 10 * time.Second}
	guard, err := newWithClock(provider, config, clock.Now)
	if err != nil {
		t.Fatalf("newWithClock() error = %v", err)
	}
	guard.Submit(context.Background(), ports.ProviderRequest{})
	clock.Advance(10 * time.Second)
	guard.Submit(context.Background(), ports.ProviderRequest{})
	if calls := provider.calls.Load(); calls != 2 {
		t.Fatalf("provider calls = %d, want 2", calls)
	}

	clock.Advance(9 * time.Second)
	assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
		message.FailureProviderDown, circuitOpenCode)
	if calls := provider.calls.Load(); calls != 2 {
		t.Fatalf("provider calls before restarted cooldown = %d, want 2", calls)
	}
	clock.Advance(time.Second)
	guard.Submit(context.Background(), ports.ProviderRequest{})
	if calls := provider.calls.Load(); calls != 3 {
		t.Fatalf("provider calls after restarted cooldown = %d, want 3", calls)
	}
}

func TestCircuitBusinessResponseResetsConsecutiveInfrastructureFailures(t *testing.T) {
	t.Parallel()
	breaker, err := newCircuitBreaker(CircuitConfig{FailureThreshold: 2, OpenDuration: time.Second})
	if err != nil {
		t.Fatalf("newCircuitBreaker() error = %v", err)
	}
	now := time.Unix(1_700_000_000, 0)
	recordDirect(t, breaker, now, failedResult(message.FailureNetwork, "NETWORK_1", true))
	recordDirect(t, breaker, now, failedResult(message.FailureRecipientRejected, "RECIPIENT", false))
	recordDirect(t, breaker, now, failedResult(message.FailureProviderDown, "DOWN_1", true))
	if state := breaker.currentState(); state != CircuitClosed {
		t.Fatalf("state after reset and one failure = %q, want CLOSED", state)
	}
	recordDirect(t, breaker, now, failedResult(message.FailureNetwork, "NETWORK_2", true))
	if state := breaker.currentState(); state != CircuitOpen {
		t.Fatalf("state after two new failures = %q, want OPEN", state)
	}
}

func TestCircuitIgnoresStaleInFlightResultAfterOpening(t *testing.T) {
	t.Parallel()
	breaker, err := newCircuitBreaker(CircuitConfig{FailureThreshold: 1, OpenDuration: time.Second})
	if err != nil {
		t.Fatalf("newCircuitBreaker() error = %v", err)
	}
	now := time.Unix(1_700_000_000, 0)
	failing, admitted := breaker.acquire(now)
	if !admitted {
		t.Fatal("failing call was not admitted")
	}
	staleSuccess, admitted := breaker.acquire(now)
	if !admitted {
		t.Fatal("second closed call was not admitted")
	}
	breaker.record(failing, failedResult(message.FailureNetwork, "NETWORK", true), now)
	breaker.record(staleSuccess, acceptedResult("late-success"), now)
	if state := breaker.currentState(); state != CircuitOpen {
		t.Fatalf("state after stale success = %q, want OPEN", state)
	}
}

func TestHalfOpenAdmissionCanceledBeforeProviderCanBeRetried(t *testing.T) {
	t.Parallel()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	provider := newMutableProvider(failedResult(message.FailureNetwork, "NETWORK", true))
	config := guardConfig(1, 0.001, 1)
	config.Circuit = CircuitConfig{FailureThreshold: 1, OpenDuration: time.Second}
	guard, err := newWithClock(provider, config, clock.Now)
	if err != nil {
		t.Fatalf("newWithClock() error = %v", err)
	}
	guard.Submit(context.Background(), ports.ProviderRequest{})

	clock.Advance(time.Second)
	assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
		message.FailureRateLimited, rateLimitedCode)
	// The first local rejection must release the half-open probe admission;
	// otherwise this call would incorrectly see CIRCUIT_OPEN.
	assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
		message.FailureRateLimited, rateLimitedCode)

	clock.Advance(999 * time.Second)
	provider.SetResult(acceptedResult("recovered"))
	if result := guard.Submit(context.Background(), ports.ProviderRequest{}); result.Outcome != ports.ProviderOutcomeAccepted {
		t.Fatalf("recovered result = %#v", result)
	}
	if state := guard.CircuitState(); state != CircuitClosed {
		t.Fatalf("CircuitState() = %q, want CLOSED", state)
	}
}

func TestClassifyCircuitResult(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		result ports.ProviderResult
		want   circuitObservation
	}{
		{name: "accepted", result: acceptedResult("accepted"), want: circuitHealthy},
		{name: "submission unknown", result: submissionUnknownResult(), want: circuitFailure},
		{name: "authentication", result: failedResult(message.FailureAuthentication, "AUTH", false), want: circuitImmediateFailure},
		{name: "rate limited", result: failedResult(message.FailureRateLimited, "RATE", true), want: circuitFailure},
		{name: "provider down", result: failedResult(message.FailureProviderDown, "DOWN", true), want: circuitFailure},
		{name: "network", result: failedResult(message.FailureNetwork, "NETWORK", true), want: circuitFailure},
		{name: "timeout", result: failedResult(message.FailureTimeoutBeforeSend, "TIMEOUT", true), want: circuitFailure},
		{name: "validation", result: failedResult(message.FailureValidation, "VALIDATION", false), want: circuitHealthy},
		{name: "recipient", result: failedResult(message.FailureRecipientRejected, "RECIPIENT", false), want: circuitHealthy},
		{name: "content", result: failedResult(message.FailureContentRejected, "CONTENT", false), want: circuitHealthy},
		{name: "internal", result: failedResult(message.FailureInternal, "INTERNAL", true), want: circuitIgnored},
		{name: "invalid result", result: ports.ProviderResult{}, want: circuitIgnored},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyCircuitResult(test.result); got != test.want {
				t.Fatalf("classifyCircuitResult() = %d, want %d", got, test.want)
			}
		})
	}
}

func recordDirect(
	t *testing.T,
	breaker *circuitBreaker,
	now time.Time,
	result ports.ProviderResult,
) {
	t.Helper()
	ticket, admitted := breaker.acquire(now)
	if !admitted {
		t.Fatal("closed circuit did not admit call")
	}
	breaker.record(ticket, result, now)
}

func failedResult(category message.FailureCategory, code string, retryable bool) ports.ProviderResult {
	failure := message.Failure{Category: category, Code: code, Retryable: retryable}
	return ports.ProviderResult{Outcome: ports.ProviderOutcomeFailed, Failure: &failure}
}

func submissionUnknownResult() ports.ProviderResult {
	failure := message.Failure{
		Category:  message.FailureSubmissionUnknown,
		Code:      "UNKNOWN",
		Retryable: false,
	}
	return ports.ProviderResult{Outcome: ports.ProviderOutcomeSubmissionUnknown, Failure: &failure}
}

type mutableProvider struct {
	mu sync.Mutex

	result  ports.ProviderResult
	block   bool
	calls   uint32
	entered chan struct{}
	release chan struct{}
}

func newMutableProvider(result ports.ProviderResult) *mutableProvider {
	return &mutableProvider{
		result:  result,
		entered: make(chan struct{}, 1),
		release: make(chan struct{}, 1),
	}
}

func (p *mutableProvider) Key() string { return "mutable-provider" }

func (p *mutableProvider) Submit(ctx context.Context, _ ports.ProviderRequest) ports.ProviderResult {
	p.mu.Lock()
	p.calls++
	result := p.result
	block := p.block
	p.mu.Unlock()
	if block {
		p.entered <- struct{}{}
		select {
		case <-ctx.Done():
			return contextDoneResult()
		case <-p.release:
		}
	}
	return result
}

func (p *mutableProvider) SetResult(result ports.ProviderResult) {
	p.mu.Lock()
	p.result = result
	p.block = false
	p.mu.Unlock()
}

func (p *mutableProvider) BlockWithResult(result ports.ProviderResult) {
	p.mu.Lock()
	p.result = result
	p.block = true
	p.mu.Unlock()
}

func (p *mutableProvider) WaitForEntry(t *testing.T) {
	t.Helper()
	select {
	case <-p.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for provider entry")
	}
}

func (p *mutableProvider) Release() { p.release <- struct{}{} }

func (p *mutableProvider) CallCount() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}
