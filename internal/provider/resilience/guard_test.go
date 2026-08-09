package resilience

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config Config
		valid  bool
	}{
		{name: "default", config: DefaultConfig(), valid: true},
		{name: "zero concurrency", config: Config{RatePerSecond: 1, Burst: 1}},
		{name: "infinite rate", config: Config{MaxConcurrent: 1, RatePerSecond: math.Inf(1), Burst: 1}},
		{name: "rate too small", config: Config{MaxConcurrent: 1, RatePerSecond: 0.0001, Burst: 1}},
		{name: "zero burst", config: Config{MaxConcurrent: 1, RatePerSecond: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.valid && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if !test.valid && !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestGuardPreservesProviderIdentityAndAcceptedResult(t *testing.T) {
	t.Parallel()
	want := acceptedResult("accepted-1")
	provider := &providerStub{result: want}
	guard, err := New(provider, DefaultConfig())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if guard.Key() != provider.Key() {
		t.Fatalf("Key() = %q, want %q", guard.Key(), provider.Key())
	}
	if got := guard.Submit(context.Background(), ports.ProviderRequest{}); got != want {
		t.Fatalf("Submit() = %#v, want %#v", got, want)
	}
}

func TestGuardBulkheadRejectsWithoutWaiting(t *testing.T) {
	t.Parallel()
	provider := newBlockingProvider()
	guard, err := New(provider, guardConfig(2, 100, 100))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	results := make(chan ports.ProviderResult, 2)
	for range 2 {
		go func() { results <- guard.Submit(context.Background(), ports.ProviderRequest{}) }()
	}
	provider.waitForEntries(t, 2)

	assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
		message.FailureRateLimited, bulkheadFullCode)
	if got := provider.calls.Load(); got != 2 {
		t.Fatalf("provider calls = %d, want 2", got)
	}

	provider.release <- struct{}{}
	provider.release <- struct{}{}
	for range 2 {
		if result := <-results; result.Outcome != ports.ProviderOutcomeAccepted {
			t.Fatalf("blocked Submit() result = %#v", result)
		}
	}
}

func TestGuardTokenBucketRefillsWithTime(t *testing.T) {
	t.Parallel()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	provider := &providerStub{result: acceptedResult("accepted")}
	guard, err := newWithClock(provider, guardConfig(1, 2, 2), clock.Now)
	if err != nil {
		t.Fatalf("newWithClock() error = %v", err)
	}

	for range 2 {
		if result := guard.Submit(context.Background(), ports.ProviderRequest{}); result.Outcome != ports.ProviderOutcomeAccepted {
			t.Fatalf("initial token result = %#v", result)
		}
	}
	assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
		message.FailureRateLimited, rateLimitedCode)

	clock.Advance(500 * time.Millisecond)
	if result := guard.Submit(context.Background(), ports.ProviderRequest{}); result.Outcome != ports.ProviderOutcomeAccepted {
		t.Fatalf("refilled token result = %#v", result)
	}
	assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
		message.FailureRateLimited, rateLimitedCode)
	if got := provider.calls.Load(); got != 3 {
		t.Fatalf("provider calls = %d, want 3", got)
	}
}

func TestGuardTokenBucketIsSafeUnderConcurrency(t *testing.T) {
	t.Parallel()
	provider := &providerStub{result: acceptedResult("accepted")}
	guard, err := New(provider, guardConfig(100, 0.001, 10))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const submissions = 100
	results := make(chan ports.ProviderResult, submissions)
	var started sync.WaitGroup
	started.Add(submissions)
	start := make(chan struct{})
	for range submissions {
		go func() {
			started.Done()
			<-start
			results <- guard.Submit(context.Background(), ports.ProviderRequest{})
		}()
	}
	started.Wait()
	close(start)

	accepted := 0
	for range submissions {
		result := <-results
		if result.Outcome == ports.ProviderOutcomeAccepted {
			accepted++
			continue
		}
		assertFailure(t, result, message.FailureRateLimited, rateLimitedCode)
	}
	if accepted != 10 || provider.calls.Load() != 10 {
		t.Fatalf("accepted/provider calls = %d/%d, want 10/10", accepted, provider.calls.Load())
	}
}

func TestGuardBulkheadRejectionDoesNotConsumeToken(t *testing.T) {
	t.Parallel()
	provider := newBlockingProvider()
	guard, err := New(provider, guardConfig(1, 0.001, 2))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	first := make(chan ports.ProviderResult, 1)
	go func() { first <- guard.Submit(context.Background(), ports.ProviderRequest{}) }()
	provider.waitForEntries(t, 1)
	assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
		message.FailureRateLimited, bulkheadFullCode)
	provider.release <- struct{}{}
	<-first

	second := make(chan ports.ProviderResult, 1)
	go func() { second <- guard.Submit(context.Background(), ports.ProviderRequest{}) }()
	provider.waitForEntries(t, 1)
	provider.release <- struct{}{}
	if result := <-second; result.Outcome != ports.ProviderOutcomeAccepted {
		t.Fatalf("remaining token result = %#v", result)
	}
	assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
		message.FailureRateLimited, rateLimitedCode)
}

func TestGuardCanceledContextDoesNotCallProviderOrConsumeToken(t *testing.T) {
	t.Parallel()
	provider := &providerStub{result: acceptedResult("accepted")}
	guard, err := New(provider, guardConfig(1, 0.001, 1))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assertFailure(t, guard.Submit(ctx, ports.ProviderRequest{}),
		message.FailureTimeoutBeforeSend, contextDoneCode)
	if got := provider.calls.Load(); got != 0 {
		t.Fatalf("provider calls after canceled context = %d, want 0", got)
	}
	if result := guard.Submit(context.Background(), ports.ProviderRequest{}); result.Outcome != ports.ProviderOutcomeAccepted {
		t.Fatalf("token after cancellation result = %#v", result)
	}
}

type providerStub struct {
	calls  atomic.Uint32
	result ports.ProviderResult
}

func (p *providerStub) Key() string { return "test-provider" }

func (p *providerStub) Submit(context.Context, ports.ProviderRequest) ports.ProviderResult {
	p.calls.Add(1)
	return p.result
}

type blockingProvider struct {
	calls   atomic.Uint32
	entered chan struct{}
	release chan struct{}
}

func newBlockingProvider() *blockingProvider {
	return &blockingProvider{
		entered: make(chan struct{}, 4),
		release: make(chan struct{}, 4),
	}
}

func (p *blockingProvider) Key() string { return "blocking-provider" }

func (p *blockingProvider) Submit(ctx context.Context, _ ports.ProviderRequest) ports.ProviderResult {
	p.calls.Add(1)
	p.entered <- struct{}{}
	select {
	case <-ctx.Done():
		return contextDoneResult()
	case <-p.release:
		return acceptedResult("accepted")
	}
}

func (p *blockingProvider) waitForEntries(t *testing.T, count int) {
	t.Helper()
	for range count {
		select {
		case <-p.entered:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for provider entry")
		}
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func acceptedResult(id string) ports.ProviderResult {
	return ports.ProviderResult{
		Outcome:           ports.ProviderOutcomeAccepted,
		ProviderMessageID: id,
	}
}

func guardConfig(maxConcurrent uint32, ratePerSecond float64, burst uint32) Config {
	return Config{
		MaxConcurrent: maxConcurrent,
		RatePerSecond: ratePerSecond,
		Burst:         burst,
		Circuit:       DefaultCircuitConfig(),
	}
}

func assertFailure(
	t *testing.T,
	result ports.ProviderResult,
	category message.FailureCategory,
	code string,
) {
	t.Helper()
	if err := result.Validate(); err != nil {
		t.Fatalf("result validation error = %v; result = %#v", err, result)
	}
	if result.Outcome != ports.ProviderOutcomeFailed || result.Failure == nil ||
		result.Failure.Category != category || result.Failure.Code != code || !result.Failure.Retryable {
		t.Fatalf("failure result = %#v, want category=%q code=%q retryable", result, category, code)
	}
}
