package ports

import (
	"errors"
	"testing"
)

func TestEventAckDispositionValid(t *testing.T) {
	for _, disposition := range []EventAckDisposition{
		EventAckAccepted,
		EventAckDuplicate,
		EventAckIgnoredStale,
	} {
		if !disposition.Valid() {
			t.Errorf("disposition %q should be valid", disposition)
		}
	}
	if EventAckDisposition("UNKNOWN").Valid() {
		t.Fatal("unknown disposition should be invalid")
	}
}

func TestDeliveryEventSubscriberErrorValidationAndCause(t *testing.T) {
	cause := errors.New("private transport detail")
	failure := NewDeliveryEventSubscriberError("GRPC_UNAVAILABLE", true, cause)
	if err := failure.Validate(); err != nil {
		t.Fatalf("valid subscriber error: %v", err)
	}
	if failure.Cause() != cause || errors.Is(failure, cause) {
		t.Fatal("cause should be observable explicitly but absent from the public unwrap chain")
	}
	invalid := NewDeliveryEventSubscriberError("unsafe code", false, nil)
	if err := invalid.Validate(); !errors.Is(err, ErrInvalidDeliveryEventSubscriberError) {
		t.Fatalf("invalid code error = %v, want ErrInvalidDeliveryEventSubscriberError", err)
	}
}
