package providermetrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	providerresilience "github.com/Zhiruosama/Email-Service/internal/provider/resilience"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestObserverExportsExpectedMetricsWithAttributeWhitelist(t *testing.T) {
	t.Parallel()
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown meter provider: %v", err)
		}
	})
	observer, err := New(meterProvider.Meter(InstrumentationScope))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx := context.Background()
	observer.RecordProviderCall(ctx, providerresilience.ProviderCallObservation{
		ProviderKey:     "smtp",
		Outcome:         ports.ProviderOutcomeFailed,
		FailureCategory: message.FailureNetwork,
		Duration:        250 * time.Millisecond,
	})
	observer.RecordProviderRejection(ctx, providerresilience.ProviderRejectionObservation{
		ProviderKey: "smtp",
		Reason:      providerresilience.RejectionCircuitOpen,
	})
	observer.RecordCircuitState(ctx, providerresilience.CircuitStateObservation{
		ProviderKey: "smtp",
		State:       providerresilience.CircuitOpen,
	})
	observer.RecordCircuitTransition(ctx, providerresilience.CircuitTransitionObservation{
		ProviderKey: "smtp",
		From:        providerresilience.CircuitClosed,
		To:          providerresilience.CircuitOpen,
		Reason:      providerresilience.TransitionFailureThreshold,
	})
	unsafeMarker := "recipient@example.com"
	observer.RecordProviderCall(ctx, providerresilience.ProviderCallObservation{
		ProviderKey:     "smtp",
		Outcome:         ports.ProviderOutcome(unsafeMarker),
		FailureCategory: message.FailureCategory(unsafeMarker),
	})
	observer.RecordProviderRejection(ctx, providerresilience.ProviderRejectionObservation{
		ProviderKey: "smtp",
		Reason:      providerresilience.RejectionReason(unsafeMarker),
	})
	observer.RecordCircuitTransition(ctx, providerresilience.CircuitTransitionObservation{
		ProviderKey: "smtp",
		From:        providerresilience.CircuitState(unsafeMarker),
		To:          providerresilience.CircuitState(unsafeMarker),
		Reason:      providerresilience.CircuitTransitionReason(unsafeMarker),
	})

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &collected); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	wantAttributes := map[string]map[string]bool{
		"mail.provider.calls": {
			"provider": true, "outcome": true, "failure_category": true,
		},
		"mail.provider.duration": {
			"provider": true, "outcome": true, "failure_category": true,
		},
		"mail.provider.rejections": {
			"provider": true, "reason": true,
		},
		"mail.provider.circuit.state": {
			"provider": true,
		},
		"mail.provider.circuit.transitions": {
			"provider": true, "from": true, "to": true, "reason": true,
		},
	}
	seen := make(map[string]bool, len(wantAttributes))
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			allowed, expected := wantAttributes[metric.Name]
			if !expected {
				t.Fatalf("unexpected metric %q", metric.Name)
			}
			seen[metric.Name] = true
			for _, attributes := range metricAttributeSets(t, metric.Data) {
				if attributes.Len() != len(allowed) {
					t.Fatalf("metric %q attributes = %v, want keys %v", metric.Name, attributes.ToSlice(), allowed)
				}
				for _, keyValue := range attributes.ToSlice() {
					if !allowed[string(keyValue.Key)] {
						t.Fatalf("metric %q contains forbidden attribute %q", metric.Name, keyValue.Key)
					}
					if strings.Contains(keyValue.Value.Emit(), unsafeMarker) {
						t.Fatalf("metric %q leaked unsafe attribute value", metric.Name)
					}
				}
			}
		}
	}
	for name := range wantAttributes {
		if !seen[name] {
			t.Errorf("metric %q was not exported", name)
		}
	}
}

func TestCircuitStateValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state providerresilience.CircuitState
		want  int64
	}{
		{state: providerresilience.CircuitClosed, want: 0},
		{state: providerresilience.CircuitHalfOpen, want: 1},
		{state: providerresilience.CircuitOpen, want: 2},
		{state: providerresilience.CircuitState("UNKNOWN"), want: -1},
	}
	for _, test := range tests {
		if got := circuitStateValue(test.state); got != test.want {
			t.Errorf("circuitStateValue(%q) = %d, want %d", test.state, got, test.want)
		}
	}
}

func TestObserverIgnoresOutOfOrderCircuitStateGauge(t *testing.T) {
	t.Parallel()
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown meter provider: %v", err)
		}
	})
	observer, err := New(meterProvider.Meter(InstrumentationScope))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx := context.Background()
	observer.RecordCircuitState(ctx, providerresilience.CircuitStateObservation{
		ProviderKey: "smtp",
		State:       providerresilience.CircuitOpen,
		Sequence:    2,
	})
	observer.RecordCircuitState(ctx, providerresilience.CircuitStateObservation{
		ProviderKey: "smtp",
		State:       providerresilience.CircuitHalfOpen,
		Sequence:    1,
	})

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &collected); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "mail.provider.circuit.state" {
				continue
			}
			gauge, ok := metric.Data.(metricdata.Gauge[int64])
			if !ok || len(gauge.DataPoints) != 1 || gauge.DataPoints[0].Value != 2 {
				t.Fatalf("circuit state gauge = %#v, want OPEN=2", metric.Data)
			}
			return
		}
	}
	t.Fatal("circuit state gauge was not collected")
}

func metricAttributeSets(t *testing.T, data metricdata.Aggregation) []attribute.Set {
	t.Helper()
	switch typed := data.(type) {
	case metricdata.Sum[int64]:
		sets := make([]attribute.Set, 0, len(typed.DataPoints))
		for _, point := range typed.DataPoints {
			sets = append(sets, point.Attributes)
		}
		return sets
	case metricdata.Histogram[float64]:
		sets := make([]attribute.Set, 0, len(typed.DataPoints))
		for _, point := range typed.DataPoints {
			sets = append(sets, point.Attributes)
		}
		return sets
	case metricdata.Gauge[int64]:
		sets := make([]attribute.Set, 0, len(typed.DataPoints))
		for _, point := range typed.DataPoints {
			sets = append(sets, point.Attributes)
		}
		return sets
	default:
		t.Fatalf("unexpected aggregation type %T", data)
		return nil
	}
}
