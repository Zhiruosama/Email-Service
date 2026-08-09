package resilience

import (
	"context"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

type RejectionReason string

const (
	RejectionContextDone  RejectionReason = "context_done"
	RejectionBulkheadFull RejectionReason = "bulkhead_full"
	RejectionRateLimited  RejectionReason = "rate_limited"
	RejectionCircuitOpen  RejectionReason = "circuit_open"
)

type CircuitTransitionReason string

const (
	TransitionFailureThreshold CircuitTransitionReason = "failure_threshold"
	TransitionAuthentication   CircuitTransitionReason = "authentication_failure"
	TransitionCooldownElapsed  CircuitTransitionReason = "cooldown_elapsed"
	TransitionProbeSucceeded   CircuitTransitionReason = "half_open_probe_succeeded"
	TransitionProbeFailed      CircuitTransitionReason = "half_open_probe_failed"
)

// ProviderCallObservation deliberately contains no request, tenant, address,
// MIME, provider response, or credential data. All fields have bounded
// cardinality when ProviderKey is sourced from the validated Provider port.
type ProviderCallObservation struct {
	ProviderKey     string
	Outcome         ports.ProviderOutcome
	FailureCategory message.FailureCategory
	Duration        time.Duration
}

type ProviderRejectionObservation struct {
	ProviderKey string
	Reason      RejectionReason
}

type CircuitStateObservation struct {
	ProviderKey string
	State       CircuitState
	Sequence    uint64
}

type CircuitTransitionObservation struct {
	ProviderKey string
	From        CircuitState
	To          CircuitState
	Reason      CircuitTransitionReason
	Sequence    uint64
}

// Observer is a best-effort telemetry boundary. It cannot reject or rewrite a
// delivery result, so monitoring availability never becomes mail availability.
type Observer interface {
	RecordProviderCall(context.Context, ProviderCallObservation)
	RecordProviderRejection(context.Context, ProviderRejectionObservation)
	RecordCircuitState(context.Context, CircuitStateObservation)
	RecordCircuitTransition(context.Context, CircuitTransitionObservation)
}

type noopObserver struct{}

func (noopObserver) RecordProviderCall(context.Context, ProviderCallObservation)           {}
func (noopObserver) RecordProviderRejection(context.Context, ProviderRejectionObservation) {}
func (noopObserver) RecordCircuitState(context.Context, CircuitStateObservation)           {}
func (noopObserver) RecordCircuitTransition(context.Context, CircuitTransitionObservation) {}
