package fake

import (
	"context"
	"testing"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

func TestProviderRecordsRequestsAndUsesControllableResult(t *testing.T) {
	t.Parallel()
	want := ports.ProviderResult{
		Outcome:           ports.ProviderOutcomeAccepted,
		ProviderMessageID: "controlled-id",
	}
	provider := New(func(_ context.Context, _ ports.ProviderRequest) ports.ProviderResult {
		return want
	})
	request := ports.ProviderRequest{AttemptID: "46000000-0000-4000-8000-000000000001"}
	if got := provider.Submit(context.Background(), request); got != want {
		t.Fatalf("Submit() = %#v, want %#v", got, want)
	}
	requests := provider.Requests()
	if len(requests) != 1 || requests[0] != request {
		t.Fatalf("recorded requests = %#v, want request", requests)
	}

	requests[0].AttemptID = "changed"
	if provider.Requests()[0].AttemptID != request.AttemptID {
		t.Fatal("Requests() exposed provider-owned slice")
	}
}

func TestProviderDefaultsToAccepted(t *testing.T) {
	t.Parallel()
	provider := New(nil)
	result := provider.Submit(context.Background(), ports.ProviderRequest{
		AttemptID: "47000000-0000-4000-8000-000000000001",
	})
	if result.Outcome != ports.ProviderOutcomeAccepted || result.ProviderMessageID == "" {
		t.Fatalf("default result = %#v, want accepted", result)
	}
}
