package fake

import (
	"context"
	"fmt"
	"strings"
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
	request := ports.ProviderRequest{
		AttemptID: "46000000-0000-4000-8000-000000000001",
		MessageID: "46000000-0000-4000-8000-000000000002",
		TenantID:  "46000000-0000-4000-8000-000000000003",
		Material: ports.DeliveryMaterial{
			EnvelopeFrom: "sender@example.com",
			EnvelopeTo:   "recipient@example.com",
			MIMEMessage:  []byte("Subject: test\r\n\r\nverification code 123456"),
		},
	}
	if got := provider.Submit(context.Background(), request); got != want {
		t.Fatalf("Submit() = %#v, want %#v", got, want)
	}
	requests := provider.Requests()
	if len(requests) != 1 || requests[0].AttemptID != request.AttemptID ||
		requests[0].MaterialBytes != len(request.Material.MIMEMessage) {
		t.Fatalf("recorded observations = %#v, want request metadata", requests)
	}
	forbidden := "123456"
	if strings.Contains(fmt.Sprintf("%#v", requests), forbidden) {
		t.Fatal("fake provider retained sensitive MIME content")
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
