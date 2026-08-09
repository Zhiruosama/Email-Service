package notification

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestWorkerLoadsAuthoritativeJournalEventAndAcceptsAllSuccessDispositions(t *testing.T) {
	t.Parallel()
	event := notificationTestEvent()
	for _, disposition := range []ports.EventAckDisposition{
		ports.EventAckAccepted,
		ports.EventAckDuplicate,
		ports.EventAckIgnoredStale,
	} {
		t.Run(string(disposition), func(t *testing.T) {
			t.Parallel()
			reader := &recordingEventReader{event: event}
			subscriber := &recordingSubscriber{disposition: disposition}
			worker, err := NewWorker(reader, subscriber, WorkerConfig{CallbackTimeout: time.Second})
			if err != nil {
				t.Fatalf("new worker: %v", err)
			}
			result, err := worker.Process(context.Background(), Command{EventID: event.ID})
			if err != nil {
				t.Fatalf("process event: %v", err)
			}
			if result.Disposition != disposition || reader.eventID != event.ID || subscriber.event.ID != event.ID {
				t.Fatalf("unexpected result/read/callback: %#v %q %#v", result, reader.eventID, subscriber.event)
			}
		})
	}
}

func TestWorkerRejectsInvalidCommandBeforeDependencies(t *testing.T) {
	t.Parallel()
	worker, err := NewWorker(noCallEventReader{}, noCallSubscriber{}, WorkerConfig{CallbackTimeout: time.Second})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	_, err = worker.Process(context.Background(), Command{EventID: "event"})
	if !errors.Is(err, ErrInvalidCommand) || ClassifyError(err) != ErrorPoison {
		t.Fatalf("invalid command error/class = %v/%s", err, ClassifyError(err))
	}
}

func TestWorkerClassifiesMissingAndMismatchedJournalEventsAsPoison(t *testing.T) {
	t.Parallel()
	event := notificationTestEvent()
	missing, err := NewWorker(
		&recordingEventReader{err: ports.ErrDeliveryEventNotFound},
		noCallSubscriber{},
		WorkerConfig{CallbackTimeout: time.Second},
	)
	if err != nil {
		t.Fatalf("new missing worker: %v", err)
	}
	if _, err := missing.Process(context.Background(), Command{EventID: event.ID}); !errors.Is(err, ErrJournalEventMissing) || ClassifyError(err) != ErrorPoison {
		t.Fatalf("missing event error/class = %v/%s", err, ClassifyError(err))
	}

	mismatched := event
	mismatched.ID = "d0000000-0000-4000-8000-000000000099"
	worker, err := NewWorker(
		&recordingEventReader{event: mismatched},
		noCallSubscriber{},
		WorkerConfig{CallbackTimeout: time.Second},
	)
	if err != nil {
		t.Fatalf("new mismatch worker: %v", err)
	}
	if _, err := worker.Process(context.Background(), Command{EventID: event.ID}); !errors.Is(err, ErrNotificationInvariant) || ClassifyError(err) != ErrorPoison {
		t.Fatalf("mismatch error/class = %v/%s", err, ClassifyError(err))
	}
}

func TestWorkerClassifiesSubscriberFailures(t *testing.T) {
	t.Parallel()
	event := notificationTestEvent()
	tests := []struct {
		name  string
		err   error
		class ErrorClass
	}{
		{
			name:  "retryable typed",
			err:   ports.NewDeliveryEventSubscriberError("GRPC_UNAVAILABLE", true, nil),
			class: ErrorTransient,
		},
		{
			name:  "permanent typed",
			err:   ports.NewDeliveryEventSubscriberError("GRPC_PERMISSION_DENIED", false, nil),
			class: ErrorPoison,
		},
		{name: "raw defaults transient", err: errors.New("transport"), class: ErrorTransient},
		{
			name:  "invalid typed is invariant",
			err:   ports.NewDeliveryEventSubscriberError("unsafe code", false, nil),
			class: ErrorPoison,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			worker, err := NewWorker(
				&recordingEventReader{event: event},
				&recordingSubscriber{err: test.err},
				WorkerConfig{CallbackTimeout: time.Second},
			)
			if err != nil {
				t.Fatalf("new worker: %v", err)
			}
			_, processErr := worker.Process(context.Background(), Command{EventID: event.ID})
			if ClassifyError(processErr) != test.class {
				t.Fatalf("error/class = %v/%s, want %s", processErr, ClassifyError(processErr), test.class)
			}
		})
	}
}

func TestWorkerBoundsCallbackWithTimeout(t *testing.T) {
	t.Parallel()
	event := notificationTestEvent()
	worker, err := NewWorker(
		&recordingEventReader{event: event},
		blockingSubscriber{},
		WorkerConfig{CallbackTimeout: 10 * time.Millisecond},
	)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	_, processErr := worker.Process(context.Background(), Command{EventID: event.ID})
	var subscriberErr *ports.DeliveryEventSubscriberError
	if !errors.As(processErr, &subscriberErr) || subscriberErr.Code != "CALLBACK_TIMEOUT" ||
		ClassifyError(processErr) != ErrorTransient {
		t.Fatalf("timeout error/class = %v/%s", processErr, ClassifyError(processErr))
	}
}

func TestWorkerRejectsInvalidDisposition(t *testing.T) {
	t.Parallel()
	event := notificationTestEvent()
	worker, err := NewWorker(
		&recordingEventReader{event: event},
		&recordingSubscriber{disposition: "UNKNOWN"},
		WorkerConfig{CallbackTimeout: time.Second},
	)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	_, processErr := worker.Process(context.Background(), Command{EventID: event.ID})
	if !errors.Is(processErr, ErrNotificationInvariant) || ClassifyError(processErr) != ErrorPoison {
		t.Fatalf("invalid disposition error/class = %v/%s", processErr, ClassifyError(processErr))
	}
}

func TestNewWorkerValidatesDependenciesAndConfig(t *testing.T) {
	t.Parallel()
	if _, err := NewWorker(noCallEventReader{}, noCallSubscriber{}, WorkerConfig{}); !errors.Is(err, ErrInvalidWorkerConfig) {
		t.Fatalf("invalid config error = %v", err)
	}
	for _, construct := range []func(){
		func() { _, _ = NewWorker(nil, noCallSubscriber{}, WorkerConfig{CallbackTimeout: time.Second}) },
		func() { _, _ = NewWorker(noCallEventReader{}, nil, WorkerConfig{CallbackTimeout: time.Second}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("constructor did not panic")
				}
			}()
			construct()
		}()
	}
}

func notificationTestEvent() ports.PersistedDeliveryEvent {
	occurredAt := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	return ports.PersistedDeliveryEvent{
		DeliveryEvent: ports.DeliveryEvent{
			ID:                "d0000000-0000-4000-8000-000000000001",
			TenantID:          "d0000000-0000-4000-8000-000000000002",
			MessageID:         "d0000000-0000-4000-8000-000000000003",
			IdempotencyKey:    "request-1",
			Status:            message.StatusProviderAccepted,
			Sequence:          4,
			AttemptNumber:     1,
			ProviderMessageID: "provider-1",
			OccurredAt:        occurredAt,
		},
		ObservedAt: occurredAt.Add(time.Second),
	}
}

type recordingEventReader struct {
	event   ports.PersistedDeliveryEvent
	err     error
	eventID string
}

func (r *recordingEventReader) GetByID(_ context.Context, eventID string) (ports.PersistedDeliveryEvent, error) {
	r.eventID = eventID
	return r.event, r.err
}

type noCallEventReader struct{}

func (noCallEventReader) GetByID(context.Context, string) (ports.PersistedDeliveryEvent, error) {
	panic("must not be called")
}

type recordingSubscriber struct {
	disposition ports.EventAckDisposition
	err         error
	event       ports.PersistedDeliveryEvent
}

func (s *recordingSubscriber) Report(
	_ context.Context,
	event ports.PersistedDeliveryEvent,
) (ports.EventAckDisposition, error) {
	s.event = event
	return s.disposition, s.err
}

type noCallSubscriber struct{}

func (noCallSubscriber) Report(context.Context, ports.PersistedDeliveryEvent) (ports.EventAckDisposition, error) {
	panic("must not be called")
}

type blockingSubscriber struct{}

func (blockingSubscriber) Report(ctx context.Context, _ ports.PersistedDeliveryEvent) (ports.EventAckDisposition, error) {
	<-ctx.Done()
	return "", ctx.Err()
}
