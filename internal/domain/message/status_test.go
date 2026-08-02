package message

import "testing"

func TestCanTransitionMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		from    Status
		to      Status
		allowed bool
	}{
		{name: "accepted to scheduled", from: StatusAccepted, to: StatusScheduled, allowed: true},
		{name: "accepted to queued", from: StatusAccepted, to: StatusQueued, allowed: true},
		{name: "scheduled to queued", from: StatusScheduled, to: StatusQueued, allowed: true},
		{name: "queued to sending", from: StatusQueued, to: StatusSending, allowed: true},
		{name: "sending to retry", from: StatusSending, to: StatusRetryScheduled, allowed: true},
		{name: "sending directly to delivered fact", from: StatusSending, to: StatusDelivered, allowed: true},
		{name: "unknown directly to bounced fact", from: StatusSubmissionUnknown, to: StatusBounced, allowed: true},
		{name: "provider accepted to delivered", from: StatusProviderAccepted, to: StatusDelivered, allowed: true},
		{name: "delivered to complained", from: StatusDelivered, to: StatusComplained, allowed: true},
		{name: "delivered cannot send again", from: StatusDelivered, to: StatusSending, allowed: false},
		{name: "canceled cannot queue", from: StatusCanceled, to: StatusQueued, allowed: false},
		{name: "bounced cannot retry", from: StatusBounced, to: StatusRetryScheduled, allowed: false},
		{name: "same state is not a transition", from: StatusQueued, to: StatusQueued, allowed: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := canTransition(test.from, test.to); got != test.allowed {
				t.Fatalf("canTransition(%s, %s) = %t, want %t", test.from, test.to, got, test.allowed)
			}
		})
	}
}

func TestStatusTerminalSemantics(t *testing.T) {
	t.Parallel()

	if StatusDelivered.Terminal() {
		t.Fatal("DELIVERED must remain observable for a later complaint")
	}
	for _, status := range []Status{
		StatusBounced,
		StatusComplained,
		StatusCanceled,
		StatusExpired,
		StatusPermanentlyFailed,
		StatusDeadLettered,
		StatusUnknownFinal,
	} {
		if !status.Terminal() {
			t.Errorf("%s should be terminal", status)
		}
	}
}
