// Package providermetrics adapts Provider resilience observations to
// OpenTelemetry instruments with an intentionally small, low-cardinality
// attribute vocabulary.
package providermetrics

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	providerresilience "github.com/Zhiruosama/Email-Service/internal/provider/resilience"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const InstrumentationScope = "github.com/Zhiruosama/Email-Service/provider"

var ErrCreateInstruments = errors.New("create provider metric instruments")

type Observer struct {
	calls              metric.Int64Counter
	duration           metric.Float64Histogram
	rejections         metric.Int64Counter
	circuitState       metric.Int64Gauge
	circuitTransitions metric.Int64Counter

	mu             sync.Mutex
	stateSequences map[string]uint64
}

var _ providerresilience.Observer = (*Observer)(nil)

func New(meter metric.Meter) (*Observer, error) {
	if meter == nil {
		return nil, fmt.Errorf("%w: meter is required", ErrCreateInstruments)
	}
	calls, err := meter.Int64Counter(
		"mail.provider.calls",
		metric.WithDescription("Provider calls that reached the external adapter"),
		metric.WithUnit("{call}"),
	)
	if err != nil {
		return nil, instrumentError("mail.provider.calls", err)
	}
	duration, err := meter.Float64Histogram(
		"mail.provider.duration",
		metric.WithDescription("Duration of calls that reached the external provider adapter"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, instrumentError("mail.provider.duration", err)
	}
	rejections, err := meter.Int64Counter(
		"mail.provider.rejections",
		metric.WithDescription("Calls rejected locally before reaching the provider adapter"),
		metric.WithUnit("{rejection}"),
	)
	if err != nil {
		return nil, instrumentError("mail.provider.rejections", err)
	}
	circuitState, err := meter.Int64Gauge(
		"mail.provider.circuit.state",
		metric.WithDescription("Current local circuit state: CLOSED=0, HALF_OPEN=1, OPEN=2"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, instrumentError("mail.provider.circuit.state", err)
	}
	circuitTransitions, err := meter.Int64Counter(
		"mail.provider.circuit.transitions",
		metric.WithDescription("Local Provider circuit state transitions"),
		metric.WithUnit("{transition}"),
	)
	if err != nil {
		return nil, instrumentError("mail.provider.circuit.transitions", err)
	}
	return &Observer{
		calls:              calls,
		duration:           duration,
		rejections:         rejections,
		circuitState:       circuitState,
		circuitTransitions: circuitTransitions,
		stateSequences:     make(map[string]uint64),
	}, nil
}

func (o *Observer) RecordProviderCall(
	ctx context.Context,
	observation providerresilience.ProviderCallObservation,
) {
	failureCategory := string(observation.FailureCategory)
	if failureCategory == "" {
		failureCategory = "none"
	} else if !observation.FailureCategory.Valid() {
		failureCategory = "unknown"
	}
	attributes := metric.WithAttributes(
		attribute.String("provider", safeProviderKey(observation.ProviderKey)),
		attribute.String("outcome", safeOutcome(observation.Outcome)),
		attribute.String("failure_category", failureCategory),
	)
	o.calls.Add(ctx, 1, attributes)
	o.duration.Record(ctx, observation.Duration.Seconds(), attributes)
}

func (o *Observer) RecordProviderRejection(
	ctx context.Context,
	observation providerresilience.ProviderRejectionObservation,
) {
	o.rejections.Add(ctx, 1, metric.WithAttributes(
		attribute.String("provider", safeProviderKey(observation.ProviderKey)),
		attribute.String("reason", safeRejectionReason(observation.Reason)),
	))
}

func (o *Observer) RecordCircuitState(
	ctx context.Context,
	observation providerresilience.CircuitStateObservation,
) {
	o.mu.Lock()
	providerKey := safeProviderKey(observation.ProviderKey)
	latest, exists := o.stateSequences[providerKey]
	if exists && observation.Sequence <= latest {
		o.mu.Unlock()
		return
	}
	o.stateSequences[providerKey] = observation.Sequence
	o.circuitState.Record(ctx, circuitStateValue(observation.State), metric.WithAttributes(
		attribute.String("provider", providerKey),
	))
	o.mu.Unlock()
}

func (o *Observer) RecordCircuitTransition(
	ctx context.Context,
	observation providerresilience.CircuitTransitionObservation,
) {
	o.circuitTransitions.Add(ctx, 1, metric.WithAttributes(
		attribute.String("provider", safeProviderKey(observation.ProviderKey)),
		attribute.String("from", safeCircuitState(observation.From)),
		attribute.String("to", safeCircuitState(observation.To)),
		attribute.String("reason", safeTransitionReason(observation.Reason)),
	))
}

func circuitStateValue(state providerresilience.CircuitState) int64 {
	switch state {
	case providerresilience.CircuitHalfOpen:
		return 1
	case providerresilience.CircuitOpen:
		return 2
	case providerresilience.CircuitClosed:
		return 0
	default:
		return -1
	}
}

func safeProviderKey(value string) string {
	if err := ports.ValidateProviderKey(value); err != nil {
		return "unknown"
	}
	return value
}

func safeOutcome(value ports.ProviderOutcome) string {
	switch value {
	case ports.ProviderOutcomeAccepted,
		ports.ProviderOutcomeFailed,
		ports.ProviderOutcomeSubmissionUnknown:
		return string(value)
	default:
		return "unknown"
	}
}

func safeRejectionReason(value providerresilience.RejectionReason) string {
	switch value {
	case providerresilience.RejectionContextDone,
		providerresilience.RejectionBulkheadFull,
		providerresilience.RejectionRateLimited,
		providerresilience.RejectionCircuitOpen:
		return string(value)
	default:
		return "unknown"
	}
}

func safeCircuitState(value providerresilience.CircuitState) string {
	switch value {
	case providerresilience.CircuitClosed,
		providerresilience.CircuitHalfOpen,
		providerresilience.CircuitOpen:
		return string(value)
	default:
		return "UNKNOWN"
	}
}

func safeTransitionReason(value providerresilience.CircuitTransitionReason) string {
	switch value {
	case providerresilience.TransitionFailureThreshold,
		providerresilience.TransitionAuthentication,
		providerresilience.TransitionCooldownElapsed,
		providerresilience.TransitionProbeSucceeded,
		providerresilience.TransitionProbeFailed:
		return string(value)
	default:
		return "unknown"
	}
}

func instrumentError(name string, err error) error {
	return fmt.Errorf("%w: %s: %v", ErrCreateInstruments, name, err)
}
