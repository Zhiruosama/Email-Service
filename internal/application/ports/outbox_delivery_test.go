package ports_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

func TestOutboxClaimQueryValidate(t *testing.T) {
	t.Parallel()

	valid := ports.OutboxClaimQuery{
		LeaseToken:    "relay-a/90000000-0000-4000-8000-000000000001",
		LeaseDuration: 30 * time.Second,
		Limit:         100,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid claim: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*ports.OutboxClaimQuery)
	}{
		{name: "token", mutate: func(query *ports.OutboxClaimQuery) { query.LeaseToken = " bad " }},
		{name: "short lease", mutate: func(query *ports.OutboxClaimQuery) { query.LeaseDuration = time.Millisecond }},
		{name: "long lease", mutate: func(query *ports.OutboxClaimQuery) { query.LeaseDuration = 2 * time.Hour }},
		{name: "zero limit", mutate: func(query *ports.OutboxClaimQuery) { query.Limit = 0 }},
		{name: "large limit", mutate: func(query *ports.OutboxClaimQuery) { query.Limit = ports.MaxOutboxDeliveryBatchSize + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query := valid
			test.mutate(&query)
			if err := query.Validate(); !errors.Is(err, ports.ErrInvalidOutboxDelivery) {
				t.Fatalf("Validate() error = %v, want ErrInvalidOutboxDelivery", err)
			}
		})
	}
}

func TestOutboxDeliveryCommandsValidate(t *testing.T) {
	t.Parallel()

	reference := ports.OutboxLeaseReference{
		EventID:       "90000000-0000-4000-8000-000000000001",
		LeaseToken:    "relay/token",
		AttemptNumber: 1,
	}
	if err := reference.Validate(); err != nil {
		t.Fatalf("valid reference: %v", err)
	}
	if err := (ports.RescheduleOutboxCommand{
		Lease: reference, Delay: time.Minute, ErrorCode: "BROKER_UNAVAILABLE",
	}).Validate(); err != nil {
		t.Fatalf("valid reschedule: %v", err)
	}
	if err := (ports.DeadLetterOutboxCommand{
		Lease: reference, ErrorCode: "UNROUTABLE",
	}).Validate(); err != nil {
		t.Fatalf("valid dead letter: %v", err)
	}

	invalidReferences := []ports.OutboxLeaseReference{
		{EventID: "bad", LeaseToken: "relay/token", AttemptNumber: 1},
		{EventID: reference.EventID, LeaseToken: "", AttemptNumber: 1},
		{EventID: reference.EventID, LeaseToken: "relay/token", AttemptNumber: 0},
		{EventID: reference.EventID, LeaseToken: "relay/token", AttemptNumber: math.MaxInt32 + 1},
	}
	for _, invalid := range invalidReferences {
		if err := invalid.Validate(); !errors.Is(err, ports.ErrInvalidOutboxDelivery) {
			t.Fatalf("invalid reference error = %v", err)
		}
	}

	for _, command := range []ports.RescheduleOutboxCommand{
		{Lease: reference, Delay: -time.Second, ErrorCode: "RETRY"},
		{Lease: reference, Delay: 25 * time.Hour, ErrorCode: "RETRY"},
		{Lease: reference, Delay: time.Second, ErrorCode: "unsafe value"},
	} {
		if err := command.Validate(); !errors.Is(err, ports.ErrInvalidOutboxDelivery) {
			t.Fatalf("invalid reschedule error = %v", err)
		}
	}
	if err := (ports.DeadLetterOutboxCommand{
		Lease: reference, ErrorCode: "unsafe value",
	}).Validate(); !errors.Is(err, ports.ErrInvalidOutboxDelivery) {
		t.Fatalf("invalid dead-letter error = %v", err)
	}
}

func TestOutboxPublishErrorIsSanitized(t *testing.T) {
	t.Parallel()

	cause := errors.New("secret broker response")
	publishError := ports.NewOutboxPublishError("BROKER_UNAVAILABLE", true, cause)
	if err := publishError.Validate(); err != nil {
		t.Fatalf("valid publish error: %v", err)
	}
	if publishError.Error() != "outbox publish failed: BROKER_UNAVAILABLE" {
		t.Fatalf("publish error text = %q", publishError.Error())
	}
	if errors.Is(publishError, cause) {
		t.Fatal("publish cause leaked through unwrap chain")
	}
	if publishError.Cause() != cause {
		t.Fatal("publish cause unavailable to observability")
	}

	invalid := ports.NewOutboxPublishError("unsafe response text", false, nil)
	if err := invalid.Validate(); !errors.Is(err, ports.ErrInvalidOutboxPublishError) {
		t.Fatalf("invalid publish error = %v", err)
	}
}
