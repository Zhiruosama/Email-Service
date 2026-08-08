package fake_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/publisher/fake"
)

func TestPublisherRecordsIsolatedPublications(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("scripted failure")
	publisher := fake.New(func(context.Context, ports.OutboxPublication) error {
		return sentinel
	})
	publication := ports.OutboxPublication{
		Event: ports.OutboxEvent{
			ID:      "90000000-0000-4000-8000-000000000001",
			Payload: []byte("{\"safe\":true}"),
		},
		AttemptNumber: 1,
	}
	if err := publisher.Publish(context.Background(), publication); !errors.Is(err, sentinel) {
		t.Fatalf("Publish() error = %v, want sentinel", err)
	}
	publication.Event.Payload[0] = 'x'

	got := publisher.Publications()
	if len(got) != 1 || string(got[0].Event.Payload) != "{\"safe\":true}" {
		t.Fatalf("recorded publications = %#v", got)
	}
	got[0].Event.Payload[0] = 'y'
	if string(publisher.Publications()[0].Event.Payload) != "{\"safe\":true}" {
		t.Fatal("Publications returned mutable internal payload")
	}
}
