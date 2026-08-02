package message

// Status is the normalized lifecycle state of one logical email.
type Status string

const (
	StatusAccepted          Status = "ACCEPTED"
	StatusScheduled         Status = "SCHEDULED"
	StatusQueued            Status = "QUEUED"
	StatusSending           Status = "SENDING"
	StatusRetryScheduled    Status = "RETRY_SCHEDULED"
	StatusSubmissionUnknown Status = "SUBMISSION_UNKNOWN"
	StatusProviderAccepted  Status = "PROVIDER_ACCEPTED"
	StatusDelivered         Status = "DELIVERED"
	StatusBounced           Status = "BOUNCED"
	StatusComplained        Status = "COMPLAINED"
	StatusCanceled          Status = "CANCELED"
	StatusExpired           Status = "EXPIRED"
	StatusPermanentlyFailed Status = "PERMANENTLY_FAILED"
	StatusDeadLettered      Status = "DEAD_LETTERED"
	StatusUnknownFinal      Status = "UNKNOWN_FINAL"
)

// Valid reports whether the status is part of the V1 state machine.
func (s Status) Valid() bool {
	switch s {
	case StatusAccepted,
		StatusScheduled,
		StatusQueued,
		StatusSending,
		StatusRetryScheduled,
		StatusSubmissionUnknown,
		StatusProviderAccepted,
		StatusDelivered,
		StatusBounced,
		StatusComplained,
		StatusCanceled,
		StatusExpired,
		StatusPermanentlyFailed,
		StatusDeadLettered,
		StatusUnknownFinal:
		return true
	default:
		return false
	}
}

// Terminal reports whether no further business transition is expected.
// Delivered is deliberately not terminal because a complaint can arrive later.
func (s Status) Terminal() bool {
	switch s {
	case StatusBounced,
		StatusComplained,
		StatusCanceled,
		StatusExpired,
		StatusPermanentlyFailed,
		StatusDeadLettered,
		StatusUnknownFinal:
		return true
	default:
		return false
	}
}

func canTransition(from, to Status) bool {
	switch from {
	case StatusAccepted:
		return oneOf(to, StatusScheduled, StatusQueued, StatusCanceled, StatusExpired)
	case StatusScheduled:
		return oneOf(to, StatusQueued, StatusCanceled, StatusExpired)
	case StatusQueued:
		return oneOf(to, StatusSending, StatusCanceled, StatusExpired)
	case StatusSending:
		return oneOf(
			to,
			StatusRetryScheduled,
			StatusSubmissionUnknown,
			StatusProviderAccepted,
			StatusDelivered,
			StatusBounced,
			StatusComplained,
			StatusPermanentlyFailed,
			StatusDeadLettered,
		)
	case StatusRetryScheduled:
		return oneOf(to, StatusQueued, StatusCanceled, StatusExpired, StatusDeadLettered)
	case StatusSubmissionUnknown:
		return oneOf(
			to,
			StatusRetryScheduled,
			StatusProviderAccepted,
			StatusDelivered,
			StatusBounced,
			StatusComplained,
			StatusPermanentlyFailed,
			StatusUnknownFinal,
		)
	case StatusProviderAccepted:
		return oneOf(to, StatusDelivered, StatusBounced, StatusComplained)
	case StatusDelivered:
		return to == StatusComplained
	default:
		return false
	}
}

func oneOf(value Status, candidates ...Status) bool {
	for _, candidate := range candidates {
		if value == candidate {
			return true
		}
	}
	return false
}
