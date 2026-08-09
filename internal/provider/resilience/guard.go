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

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

var _ ports.EmailProvider = (*Guard)(nil)

func New(provider ports.EmailProvider, config Config) (*Guard, error) {
	return newWithClock(provider, config, time.Now)
}

func newWithClock(
	provider ports.EmailProvider,
	config Config,
	now func() time.Time,
) (*Guard, error) {
	if provider == nil {
		return nil, invalidConfig("provider is required")
	}
	if now == nil {
		return nil, invalidConfig("clock is required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	burst := float64(config.Burst)
	breaker, err := newCircuitBreaker(config.Circuit)
	if err != nil {
		return nil, err
	}
	return &Guard{
		provider: provider,
		slots:    make(chan struct{}, config.MaxConcurrent),
		rate:     config.RatePerSecond,
		burst:    burst,
		now:      now,
		breaker:  breaker,
		tokens:   burst,
		last:     now(),
	}, nil
}

func (g *Guard) Key() string { return g.provider.Key() }

// CircuitState exposes safe state for future metrics and health diagnostics.
func (g *Guard) CircuitState() CircuitState { return g.breaker.currentState() }

func (g *Guard) Submit(
	ctx context.Context,
	request ports.ProviderRequest,
) ports.ProviderResult {
	if ctx.Err() != nil {
		return contextDoneResult()
	}
	ticket, admitted := g.breaker.acquire(g.now())
	if !admitted {
		return circuitOpenResult()
	}

	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		g.breaker.cancel(ticket)
		return retryableRateLimitResult(bulkheadFullCode)
	}

	// Cancellation after acquiring a slot must not consume rate capacity.
	if ctx.Err() != nil {
		g.breaker.cancel(ticket)
		return contextDoneResult()
	}
	if !g.allow(g.now()) {
		g.breaker.cancel(ticket)
		return retryableRateLimitResult(rateLimitedCode)
	}
	result := g.provider.Submit(ctx, request)
	g.breaker.record(ticket, result, g.now())
	return result
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
