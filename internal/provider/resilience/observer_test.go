package resilience

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestGuardObserverRecordsOnlyExternalCallDurationAndSafeResult(t *testing.T) {
	t.Parallel()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	observer := &recordingObserver{}
	provider := &advancingProvider{
		clock:   clock,
		advance: 250 * time.Millisecond,
		result:  acceptedResult("accepted"),
	}
	guard, err := newWithClockAndObserver(
		provider,
		guardConfig(2, 100, 100),
		clock.Now,
		observer,
	)
	if err != nil {
		t.Fatalf("newWithClockAndObserver() error = %v", err)
	}

	result := guard.Submit(context.Background(), ports.ProviderRequest{})
	if result.Outcome != ports.ProviderOutcomeAccepted {
		t.Fatalf("Submit() = %#v", result)
	}
	calls, rejections, states, transitions := observer.Snapshot()
	if len(calls) != 1 || calls[0].ProviderKey != provider.Key() ||
		calls[0].Outcome != ports.ProviderOutcomeAccepted || calls[0].FailureCategory != "" ||
		calls[0].Duration != 250*time.Millisecond {
		t.Fatalf("call observations = %#v", calls)
	}
	if len(rejections) != 0 || len(transitions) != 0 {
		t.Fatalf("unexpected rejections/transitions = %#v/%#v", rejections, transitions)
	}
	if len(states) != 1 || states[0].State != CircuitClosed {
		t.Fatalf("initial circuit states = %#v", states)
	}
}

func TestGuardObserverRecordsCircuitTransitionsAndRejections(t *testing.T) {
	t.Parallel()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	observer := &recordingObserver{}
	provider := newMutableProvider(failedResult(message.FailureNetwork, "NETWORK", true))
	config := guardConfig(2, 100, 100)
	config.Circuit = CircuitConfig{FailureThreshold: 1, OpenDuration: time.Second}
	guard, err := newWithClockAndObserver(provider, config, clock.Now, observer)
	if err != nil {
		t.Fatalf("newWithClockAndObserver() error = %v", err)
	}

	guard.Submit(context.Background(), ports.ProviderRequest{})
	assertFailure(t, guard.Submit(context.Background(), ports.ProviderRequest{}),
		message.FailureProviderDown, circuitOpenCode)
	clock.Advance(time.Second)
	provider.SetResult(acceptedResult("recovered"))
	guard.Submit(context.Background(), ports.ProviderRequest{})

	calls, rejections, states, transitions := observer.Snapshot()
	if len(calls) != 2 || len(rejections) != 1 || rejections[0].Reason != RejectionCircuitOpen {
		t.Fatalf("calls/rejections = %#v/%#v", calls, rejections)
	}
	wantStates := []CircuitState{CircuitClosed, CircuitOpen, CircuitHalfOpen, CircuitClosed}
	if len(states) != len(wantStates) {
		t.Fatalf("states = %#v, want %v", states, wantStates)
	}
	for index, want := range wantStates {
		if states[index].State != want || states[index].Sequence != uint64(index) {
			t.Fatalf("state[%d] = %#v, want state=%q sequence=%d", index, states[index], want, index)
		}
	}
	wantTransitions := []struct {
		from   CircuitState
		to     CircuitState
		reason CircuitTransitionReason
	}{
		{CircuitClosed, CircuitOpen, TransitionFailureThreshold},
		{CircuitOpen, CircuitHalfOpen, TransitionCooldownElapsed},
		{CircuitHalfOpen, CircuitClosed, TransitionProbeSucceeded},
	}
	if len(transitions) != len(wantTransitions) {
		t.Fatalf("transitions = %#v", transitions)
	}
	for index, want := range wantTransitions {
		got := transitions[index]
		if got.From != want.from || got.To != want.to || got.Reason != want.reason ||
			got.Sequence != uint64(index+1) {
			t.Fatalf("transition[%d] = %#v, want %#v", index, got, want)
		}
	}
}

func TestGuardObserverRecordsEveryLocalRejectionReason(t *testing.T) {
	t.Parallel()

	t.Run("context", func(t *testing.T) {
		observer := &recordingObserver{}
		guard, err := NewWithObserver(
			&providerStub{result: acceptedResult("accepted")},
			guardConfig(1, 100, 1),
			observer,
		)
		if err != nil {
			t.Fatalf("NewWithObserver() error = %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		guard.Submit(ctx, ports.ProviderRequest{})
		assertOnlyRejection(t, observer, RejectionContextDone)
	})

	t.Run("rate", func(t *testing.T) {
		observer := &recordingObserver{}
		guard, err := NewWithObserver(
			&providerStub{result: acceptedResult("accepted")},
			guardConfig(1, 0.001, 1),
			observer,
		)
		if err != nil {
			t.Fatalf("NewWithObserver() error = %v", err)
		}
		guard.Submit(context.Background(), ports.ProviderRequest{})
		guard.Submit(context.Background(), ports.ProviderRequest{})
		_, rejections, _, _ := observer.Snapshot()
		if len(rejections) != 1 || rejections[0].Reason != RejectionRateLimited {
			t.Fatalf("rejections = %#v", rejections)
		}
	})

	t.Run("bulkhead", func(t *testing.T) {
		observer := &recordingObserver{}
		provider := newBlockingProvider()
		guard, err := NewWithObserver(provider, guardConfig(1, 100, 2), observer)
		if err != nil {
			t.Fatalf("NewWithObserver() error = %v", err)
		}
		first := make(chan ports.ProviderResult, 1)
		go func() { first <- guard.Submit(context.Background(), ports.ProviderRequest{}) }()
		provider.waitForEntries(t, 1)
		guard.Submit(context.Background(), ports.ProviderRequest{})
		provider.release <- struct{}{}
		<-first
		_, rejections, _, _ := observer.Snapshot()
		if len(rejections) != 1 || rejections[0].Reason != RejectionBulkheadFull {
			t.Fatalf("rejections = %#v", rejections)
		}
	})
}

func TestGuardObserverIsSafeUnderConcurrentSubmissions(t *testing.T) {
	t.Parallel()
	observer := &recordingObserver{}
	provider := &providerStub{result: acceptedResult("accepted")}
	guard, err := NewWithObserver(provider, guardConfig(100, 0.001, 10), observer)
	if err != nil {
		t.Fatalf("NewWithObserver() error = %v", err)
	}

	const submissions = 100
	var wait sync.WaitGroup
	wait.Add(submissions)
	for range submissions {
		go func() {
			defer wait.Done()
			guard.Submit(context.Background(), ports.ProviderRequest{})
		}()
	}
	wait.Wait()
	calls, rejections, _, _ := observer.Snapshot()
	if len(calls) != 10 || len(rejections) != submissions-10 {
		t.Fatalf("calls/rejections = %d/%d, want 10/90", len(calls), len(rejections))
	}
}

func assertOnlyRejection(t *testing.T, observer *recordingObserver, want RejectionReason) {
	t.Helper()
	calls, rejections, _, transitions := observer.Snapshot()
	if len(calls) != 0 || len(transitions) != 0 || len(rejections) != 1 || rejections[0].Reason != want {
		t.Fatalf("calls/rejections/transitions = %#v/%#v/%#v", calls, rejections, transitions)
	}
}

type advancingProvider struct {
	clock   *fakeClock
	advance time.Duration
	result  ports.ProviderResult
}

func (p *advancingProvider) Key() string { return "advancing-provider" }

func (p *advancingProvider) Submit(context.Context, ports.ProviderRequest) ports.ProviderResult {
	p.clock.Advance(p.advance)
	return p.result
}

type recordingObserver struct {
	mu sync.Mutex

	calls       []ProviderCallObservation
	rejections  []ProviderRejectionObservation
	states      []CircuitStateObservation
	transitions []CircuitTransitionObservation
}

func (o *recordingObserver) RecordProviderCall(_ context.Context, observation ProviderCallObservation) {
	o.mu.Lock()
	o.calls = append(o.calls, observation)
	o.mu.Unlock()
}

func (o *recordingObserver) RecordProviderRejection(_ context.Context, observation ProviderRejectionObservation) {
	o.mu.Lock()
	o.rejections = append(o.rejections, observation)
	o.mu.Unlock()
}

func (o *recordingObserver) RecordCircuitState(_ context.Context, observation CircuitStateObservation) {
	o.mu.Lock()
	o.states = append(o.states, observation)
	o.mu.Unlock()
}

func (o *recordingObserver) RecordCircuitTransition(_ context.Context, observation CircuitTransitionObservation) {
	o.mu.Lock()
	o.transitions = append(o.transitions, observation)
	o.mu.Unlock()
}

func (o *recordingObserver) Snapshot() (
	[]ProviderCallObservation,
	[]ProviderRejectionObservation,
	[]CircuitStateObservation,
	[]CircuitTransitionObservation,
) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]ProviderCallObservation(nil), o.calls...),
		append([]ProviderRejectionObservation(nil), o.rejections...),
		append([]CircuitStateObservation(nil), o.states...),
		append([]CircuitTransitionObservation(nil), o.transitions...)
}
