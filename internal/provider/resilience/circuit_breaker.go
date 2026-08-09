package resilience

import (
	"sync"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

const (
	maxCircuitFailureThreshold = 100_000
	minCircuitOpenDuration     = 100 * time.Millisecond
	maxCircuitOpenDuration     = 24 * time.Hour
)

type CircuitState string

const (
	CircuitClosed   CircuitState = "CLOSED"
	CircuitOpen     CircuitState = "OPEN"
	CircuitHalfOpen CircuitState = "HALF_OPEN"
)

type CircuitConfig struct {
	FailureThreshold uint32
	OpenDuration     time.Duration
}

func DefaultCircuitConfig() CircuitConfig {
	return CircuitConfig{
		FailureThreshold: 5,
		OpenDuration:     30 * time.Second,
	}
}

func (c CircuitConfig) Validate() error {
	if c.FailureThreshold == 0 || c.FailureThreshold > maxCircuitFailureThreshold {
		return invalidConfig("circuit failure threshold must be in range 1..100000")
	}
	if c.OpenDuration < minCircuitOpenDuration || c.OpenDuration > maxCircuitOpenDuration {
		return invalidConfig("circuit open duration must be in range 100ms..24h")
	}
	return nil
}

type circuitTicket struct {
	epoch    uint64
	halfOpen bool
}

type circuitObservation uint8

const (
	circuitIgnored circuitObservation = iota
	circuitHealthy
	circuitFailure
	circuitImmediateFailure
)

type circuitBreaker struct {
	mu sync.Mutex

	config           CircuitConfig
	state            CircuitState
	epoch            uint64
	consecutiveFails uint32
	openUntil        time.Time
	halfOpenInFlight bool
}

func newCircuitBreaker(config CircuitConfig) (*circuitBreaker, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &circuitBreaker{config: config, state: CircuitClosed}, nil
}

// acquire returns a logical admission ticket. CLOSED allows normal calls;
// HALF_OPEN permits one probe; OPEN rejects until its cooldown has elapsed.
func (b *circuitBreaker) acquire(now time.Time) (circuitTicket, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == CircuitOpen {
		if now.Before(b.openUntil) {
			return circuitTicket{}, false
		}
		b.state = CircuitHalfOpen
		b.epoch++
		b.halfOpenInFlight = false
	}

	if b.state == CircuitHalfOpen {
		if b.halfOpenInFlight {
			return circuitTicket{}, false
		}
		b.halfOpenInFlight = true
		return circuitTicket{epoch: b.epoch, halfOpen: true}, true
	}

	return circuitTicket{epoch: b.epoch}, true
}

// cancel releases an admitted half-open probe that never reached the provider,
// for example because the local bulkhead or token bucket rejected it.
func (b *circuitBreaker) cancel(ticket circuitTicket) {
	if !ticket.halfOpen {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == CircuitHalfOpen && b.epoch == ticket.epoch {
		b.halfOpenInFlight = false
	}
}

func (b *circuitBreaker) record(
	ticket circuitTicket,
	result ports.ProviderResult,
	now time.Time,
) {
	observation := classifyCircuitResult(result)

	b.mu.Lock()
	defer b.mu.Unlock()
	if ticket.epoch != b.epoch {
		return
	}

	if ticket.halfOpen {
		if b.state != CircuitHalfOpen || !b.halfOpenInFlight {
			return
		}
		b.halfOpenInFlight = false
		if observation == circuitHealthy {
			b.close()
			return
		}
		// An ignored invariant result proves neither recovery nor continued
		// failure. Reopening avoids an unbounded half-open probe loop.
		b.open(now)
		return
	}

	if b.state != CircuitClosed {
		return
	}
	switch observation {
	case circuitHealthy:
		b.consecutiveFails = 0
	case circuitFailure:
		b.consecutiveFails++
		if b.consecutiveFails >= b.config.FailureThreshold {
			b.open(now)
		}
	case circuitImmediateFailure:
		b.open(now)
	case circuitIgnored:
	}
}

func (b *circuitBreaker) currentState() CircuitState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *circuitBreaker) open(now time.Time) {
	b.state = CircuitOpen
	b.epoch++
	b.consecutiveFails = 0
	b.openUntil = now.Add(b.config.OpenDuration)
	b.halfOpenInFlight = false
}

func (b *circuitBreaker) close() {
	b.state = CircuitClosed
	b.epoch++
	b.consecutiveFails = 0
	b.openUntil = time.Time{}
	b.halfOpenInFlight = false
}

func classifyCircuitResult(result ports.ProviderResult) circuitObservation {
	if err := result.Validate(); err != nil {
		return circuitIgnored
	}
	switch result.Outcome {
	case ports.ProviderOutcomeAccepted:
		return circuitHealthy
	case ports.ProviderOutcomeSubmissionUnknown:
		return circuitFailure
	case ports.ProviderOutcomeFailed:
		switch result.Failure.Category {
		case message.FailureAuthentication:
			return circuitImmediateFailure
		case message.FailureRateLimited,
			message.FailureProviderDown,
			message.FailureNetwork,
			message.FailureTimeoutBeforeSend:
			return circuitFailure
		case message.FailureValidation,
			message.FailureRecipientRejected,
			message.FailureContentRejected:
			return circuitHealthy
		case message.FailureSubmissionUnknown:
			return circuitFailure
		case message.FailureInternal:
			return circuitIgnored
		}
	}
	return circuitIgnored
}
