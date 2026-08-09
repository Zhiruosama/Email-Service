// Package resilience protects an EmailProvider from local overload without
// creating another in-memory work queue. Rejected calls are normalized into
// retryable provider results so the durable delivery workflow owns retrying.
package resilience

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

const (
	bulkheadFullCode = "LOCAL_PROVIDER_BULKHEAD_FULL"
	rateLimitedCode  = "LOCAL_PROVIDER_RATE_LIMITED"
	contextDoneCode  = "LOCAL_PROVIDER_CONTEXT_DONE"
	circuitOpenCode  = "LOCAL_PROVIDER_CIRCUIT_OPEN"

	maxConcurrencyLimit = 10_000
	minRatePerSecond    = 0.001
	maxRatePerSecond    = 100_000
	maxBurstLimit       = 100_000
)

var ErrInvalidConfig = errors.New("invalid provider resilience configuration")

// Config contains per-process protection limits. RatePerSecond and Burst are
// local to one service instance; they are not a distributed tenant quota.
type Config struct {
	MaxConcurrent uint32
	RatePerSecond float64
	Burst         uint32
	Circuit       CircuitConfig
}

func DefaultConfig() Config {
	return Config{
		MaxConcurrent: 2,
		RatePerSecond: 1,
		Burst:         2,
		Circuit:       DefaultCircuitConfig(),
	}
}

func (c Config) Validate() error {
	if c.MaxConcurrent == 0 || c.MaxConcurrent > maxConcurrencyLimit {
		return invalidConfig("max concurrent must be in range 1..10000")
	}
	if math.IsNaN(c.RatePerSecond) || math.IsInf(c.RatePerSecond, 0) ||
		c.RatePerSecond < minRatePerSecond || c.RatePerSecond > maxRatePerSecond {
		return invalidConfig("rate per second must be finite and in range 0.001..100000")
	}
	if c.Burst == 0 || c.Burst > maxBurstLimit {
		return invalidConfig("burst must be in range 1..100000")
	}
	if err := c.Circuit.Validate(); err != nil {
		return err
	}
	return nil
}

// Guard decorates an EmailProvider with a non-blocking concurrency bulkhead
// and a local token bucket. It retains neither requests nor MIME bytes.
type Guard struct {
	provider ports.EmailProvider
	slots    chan struct{}
	rate     float64
	burst    float64
	now      func() time.Time
	breaker  *circuitBreaker
	observer Observer

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

var _ ports.EmailProvider = (*Guard)(nil)

func New(provider ports.EmailProvider, config Config) (*Guard, error) {
	return NewWithObserver(provider, config, noopObserver{})
}

func NewWithObserver(
	provider ports.EmailProvider,
	config Config,
	observer Observer,
) (*Guard, error) {
	return newWithClockAndObserver(provider, config, time.Now, observer)
}

func newWithClock(
	provider ports.EmailProvider,
	config Config,
	now func() time.Time,
) (*Guard, error) {
	return newWithClockAndObserver(provider, config, now, noopObserver{})
}

func newWithClockAndObserver(
	provider ports.EmailProvider,
	config Config,
	now func() time.Time,
	observer Observer,
) (*Guard, error) {
	if provider == nil {
		return nil, invalidConfig("provider is required")
	}
	if err := ports.ValidateProviderKey(provider.Key()); err != nil {
		return nil, invalidConfig("provider key is invalid")
	}
	if now == nil {
		return nil, invalidConfig("clock is required")
	}
	if observer == nil {
		return nil, invalidConfig("observer is required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	burst := float64(config.Burst)
	breaker, err := newCircuitBreaker(config.Circuit)
	if err != nil {
		return nil, err
	}
	guard := &Guard{
		provider: provider,
		slots:    make(chan struct{}, config.MaxConcurrent),
		rate:     config.RatePerSecond,
		burst:    burst,
		now:      now,
		breaker:  breaker,
		observer: observer,
		tokens:   burst,
		last:     now(),
	}
	guard.observer.RecordCircuitState(context.Background(), CircuitStateObservation{
		ProviderKey: guard.Key(),
		State:       CircuitClosed,
		Sequence:    0,
	})
	return guard, nil
}

func (g *Guard) Key() string { return g.provider.Key() }

// CircuitState exposes safe state for future metrics and health diagnostics.
func (g *Guard) CircuitState() CircuitState { return g.breaker.currentState() }

func (g *Guard) Submit(
	ctx context.Context,
	request ports.ProviderRequest,
) ports.ProviderResult {
	if ctx.Err() != nil {
		g.recordRejection(ctx, RejectionContextDone)
		return contextDoneResult()
	}
	ticket, admitted, transition := g.breaker.acquire(g.now())
	g.recordTransition(ctx, transition)
	if !admitted {
		g.recordRejection(ctx, RejectionCircuitOpen)
		return circuitOpenResult()
	}

	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		g.breaker.cancel(ticket)
		g.recordRejection(ctx, RejectionBulkheadFull)
		return retryableRateLimitResult(bulkheadFullCode)
	}

	// Cancellation after acquiring a slot must not consume rate capacity.
	if ctx.Err() != nil {
		g.breaker.cancel(ticket)
		g.recordRejection(ctx, RejectionContextDone)
		return contextDoneResult()
	}
	if !g.allow(g.now()) {
		g.breaker.cancel(ticket)
		g.recordRejection(ctx, RejectionRateLimited)
		return retryableRateLimitResult(rateLimitedCode)
	}
	startedAt := g.now()
	result := g.provider.Submit(ctx, request)
	finishedAt := g.now()
	duration := finishedAt.Sub(startedAt)
	if duration < 0 {
		duration = 0
	}
	g.observer.RecordProviderCall(ctx, ProviderCallObservation{
		ProviderKey:     g.Key(),
		Outcome:         result.Outcome,
		FailureCategory: providerFailureCategory(result),
		Duration:        duration,
	})
	g.recordTransition(ctx, g.breaker.record(ticket, result, finishedAt))
	return result
}

func (g *Guard) recordRejection(ctx context.Context, reason RejectionReason) {
	g.observer.RecordProviderRejection(ctx, ProviderRejectionObservation{
		ProviderKey: g.Key(),
		Reason:      reason,
	})
}

func (g *Guard) recordTransition(ctx context.Context, transition *circuitTransition) {
	if transition == nil {
		return
	}
	g.observer.RecordCircuitTransition(ctx, CircuitTransitionObservation{
		ProviderKey: g.Key(),
		From:        transition.from,
		To:          transition.to,
		Reason:      transition.reason,
		Sequence:    transition.epoch,
	})
	g.observer.RecordCircuitState(ctx, CircuitStateObservation{
		ProviderKey: g.Key(),
		State:       transition.to,
		Sequence:    transition.epoch,
	})
}

func providerFailureCategory(result ports.ProviderResult) message.FailureCategory {
	if result.Failure == nil {
		return ""
	}
	return result.Failure.Category
}

func (g *Guard) allow(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if now.After(g.last) {
		g.tokens += now.Sub(g.last).Seconds() * g.rate
		if g.tokens > g.burst {
			g.tokens = g.burst
		}
		g.last = now
	}
	if g.tokens < 1 {
		return false
	}
	g.tokens--
	return true
}

func retryableRateLimitResult(code string) ports.ProviderResult {
	failure := message.Failure{
		Category:  message.FailureRateLimited,
		Code:      code,
		Retryable: true,
	}
	return ports.ProviderResult{
		Outcome: ports.ProviderOutcomeFailed,
		Failure: &failure,
	}
}

func contextDoneResult() ports.ProviderResult {
	failure := message.Failure{
		Category:  message.FailureTimeoutBeforeSend,
		Code:      contextDoneCode,
		Retryable: true,
	}
	return ports.ProviderResult{
		Outcome: ports.ProviderOutcomeFailed,
		Failure: &failure,
	}
}

func circuitOpenResult() ports.ProviderResult {
	failure := message.Failure{
		Category:  message.FailureProviderDown,
		Code:      circuitOpenCode,
		Retryable: true,
	}
	return ports.ProviderResult{
		Outcome: ports.ProviderOutcomeFailed,
		Failure: &failure,
	}
}

func invalidConfig(detail string) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, detail)
}
