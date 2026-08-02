package message_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestScheduledMessageCanQueueAndStartSending(t *testing.T) {
	t.Parallel()

	m := newScheduled(t, 3)
	if err := m.Queue(baseTime.Add(10 * time.Minute)); err != nil {
		t.Fatalf("Queue() error = %v", err)
	}
	if m.Status() != message.StatusQueued || m.DispatchGeneration() != 1 {
		t.Fatalf("after queue: status=%s generation=%d", m.Status(), m.DispatchGeneration())
	}
	if err := m.StartSending(1, baseTime.Add(11*time.Minute)); err != nil {
		t.Fatalf("StartSending() error = %v", err)
	}
	if m.Status() != message.StatusSending || m.AttemptCount() != 1 {
		t.Fatalf("after start: status=%s attempts=%d", m.Status(), m.AttemptCount())
	}
}

func TestStartSendingRejectsStaleGenerationWithoutMutation(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 3)
	before := m.Snapshot()
	err := m.StartSending(99, baseTime.Add(time.Minute))
	if !errors.Is(err, message.ErrStaleDispatchGeneration) {
		t.Fatalf("StartSending() error = %v, want stale generation", err)
	}
	if !reflect.DeepEqual(m.Snapshot(), before) {
		t.Fatalf("message mutated on error\n got: %+v\nwant: %+v", m.Snapshot(), before)
	}
}

func TestStartSendingRejectsDeadlineWithoutMutation(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 3)
	before := m.Snapshot()
	err := m.StartSending(m.DispatchGeneration(), m.DispatchDeadline())
	if !errors.Is(err, message.ErrDispatchDeadlineExceeded) {
		t.Fatalf("StartSending() error = %v, want deadline exceeded", err)
	}
	if !reflect.DeepEqual(m.Snapshot(), before) {
		t.Fatal("message mutated after deadline rejection")
	}
}

func TestRetryLifecycleIncrementsGenerationAndAttempts(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 3)
	if err := m.StartSending(1, baseTime.Add(time.Minute)); err != nil {
		t.Fatalf("StartSending() error = %v", err)
	}
	failure := retryableNetworkFailure()
	next := baseTime.Add(5 * time.Minute)
	if err := m.ScheduleRetry(failure, next, baseTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("ScheduleRetry() error = %v", err)
	}
	if m.Status() != message.StatusRetryScheduled || m.NextAttemptAt() == nil || !m.NextAttemptAt().Equal(next) {
		t.Fatalf("retry state invalid: %+v", m.Snapshot())
	}
	if err := m.Queue(next); err != nil {
		t.Fatalf("Queue() retry error = %v", err)
	}
	if m.DispatchGeneration() != 2 {
		t.Fatalf("generation = %d, want 2", m.DispatchGeneration())
	}
	if err := m.StartSending(2, next.Add(time.Second)); err != nil {
		t.Fatalf("second StartSending() error = %v", err)
	}
	if m.AttemptCount() != 2 {
		t.Fatalf("attempt count = %d, want 2", m.AttemptCount())
	}
}

func TestScheduleRetryRejectsExhaustedBudget(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 1)
	if err := m.StartSending(1, baseTime.Add(time.Minute)); err != nil {
		t.Fatalf("StartSending() error = %v", err)
	}
	before := m.Snapshot()
	err := m.ScheduleRetry(retryableNetworkFailure(), baseTime.Add(5*time.Minute), baseTime.Add(2*time.Minute))
	if !errors.Is(err, message.ErrAttemptLimitReached) {
		t.Fatalf("ScheduleRetry() error = %v, want attempt limit", err)
	}
	if !reflect.DeepEqual(m.Snapshot(), before) {
		t.Fatal("message mutated after attempt-limit rejection")
	}
	if err := m.MarkDeadLettered(retryableNetworkFailure(), baseTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("MarkDeadLettered() error = %v", err)
	}
	if m.Status() != message.StatusDeadLettered {
		t.Fatalf("status = %s, want DEAD_LETTERED", m.Status())
	}
}

func TestScheduleRetryRejectsNonRetryableAndLateTimes(t *testing.T) {
	t.Parallel()

	t.Run("non-retryable", func(t *testing.T) {
		m := newImmediate(t, 3)
		mustStart(t, m)
		failure := message.Failure{Category: message.FailureRecipientRejected, Code: "smtp.recipient_rejected"}
		before := m.Snapshot()
		err := m.ScheduleRetry(failure, baseTime.Add(5*time.Minute), baseTime.Add(2*time.Minute))
		if !errors.Is(err, message.ErrFailureNotRetryable) {
			t.Fatalf("error = %v, want non-retryable", err)
		}
		if !reflect.DeepEqual(m.Snapshot(), before) {
			t.Fatal("message mutated after non-retryable failure")
		}
	})

	t.Run("retry at deadline", func(t *testing.T) {
		m := newImmediate(t, 3)
		mustStart(t, m)
		err := m.ScheduleRetry(retryableNetworkFailure(), m.DispatchDeadline(), baseTime.Add(2*time.Minute))
		if !errors.Is(err, message.ErrInvalidRetryTime) {
			t.Fatalf("error = %v, want invalid retry time", err)
		}
	})
}

func TestQueueRejectsDeadlineAndInvalidSourceWithoutMutation(t *testing.T) {
	t.Parallel()

	t.Run("deadline", func(t *testing.T) {
		m := newScheduled(t, 3)
		before := m.Snapshot()
		err := m.Queue(m.DispatchDeadline())
		if !errors.Is(err, message.ErrDispatchDeadlineExceeded) {
			t.Fatalf("Queue() error = %v, want deadline exceeded", err)
		}
		if !reflect.DeepEqual(m.Snapshot(), before) {
			t.Fatal("message mutated after deadline rejection")
		}
	})

	t.Run("invalid source", func(t *testing.T) {
		m := newImmediate(t, 3)
		mustStart(t, m)
		err := m.Queue(baseTime.Add(2 * time.Minute))
		if !errors.Is(err, message.ErrInvalidTransition) {
			t.Fatalf("Queue() error = %v, want invalid transition", err)
		}
	})
}

func TestCancelIsIdempotentAndTooLateAfterSending(t *testing.T) {
	t.Parallel()

	t.Run("idempotent", func(t *testing.T) {
		m := newImmediate(t, 3)
		changed, err := m.Cancel("user.requested", baseTime.Add(time.Minute))
		if err != nil || !changed {
			t.Fatalf("first Cancel() = (%t, %v), want (true, nil)", changed, err)
		}
		sequence := m.LatestSequence()
		changed, err = m.Cancel("user.requested", baseTime.Add(2*time.Minute))
		if err != nil || changed {
			t.Fatalf("second Cancel() = (%t, %v), want (false, nil)", changed, err)
		}
		if m.LatestSequence() != sequence {
			t.Fatal("duplicate cancel changed sequence")
		}
	})

	t.Run("too late", func(t *testing.T) {
		m := newImmediate(t, 3)
		mustStart(t, m)
		changed, err := m.Cancel("user.requested", baseTime.Add(2*time.Minute))
		if changed || !errors.Is(err, message.ErrTooLateToCancel) {
			t.Fatalf("Cancel() = (%t, %v), want too late", changed, err)
		}
	})
}

func TestExpireRequiresDeadlineAndIsIdempotent(t *testing.T) {
	t.Parallel()

	m := newScheduled(t, 3)
	changed, err := m.Expire(m.DispatchDeadline().Add(-time.Nanosecond))
	if changed || !errors.Is(err, message.ErrDispatchDeadlineNotMet) {
		t.Fatalf("early Expire() = (%t, %v)", changed, err)
	}
	changed, err = m.Expire(m.DispatchDeadline())
	if err != nil || !changed || m.Status() != message.StatusExpired {
		t.Fatalf("deadline Expire() = (%t, %v), status=%s", changed, err, m.Status())
	}
	changed, err = m.Expire(m.DispatchDeadline().Add(time.Minute))
	if err != nil || changed {
		t.Fatalf("duplicate Expire() = (%t, %v), want (false, nil)", changed, err)
	}
}

func TestSubmissionUnknownCanResolveToRetry(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 3)
	mustStart(t, m)
	unknown := message.Failure{
		Category:  message.FailureSubmissionUnknown,
		Code:      "submission.result_unknown",
		Retryable: true,
	}
	if err := m.MarkSubmissionUnknown(unknown, baseTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("MarkSubmissionUnknown() error = %v", err)
	}
	if err := m.ScheduleRetry(unknown, baseTime.Add(5*time.Minute), baseTime.Add(3*time.Minute)); err != nil {
		t.Fatalf("ScheduleRetry() error = %v", err)
	}
	if m.Status() != message.StatusRetryScheduled {
		t.Fatalf("status = %s, want RETRY_SCHEDULED", m.Status())
	}
}

func TestTerminalFailurePaths(t *testing.T) {
	t.Parallel()

	t.Run("permanent failure", func(t *testing.T) {
		m := newImmediate(t, 3)
		mustStart(t, m)
		failure := message.Failure{
			Category: message.FailureRecipientRejected,
			Code:     "smtp.recipient_rejected",
		}
		if err := m.MarkPermanentlyFailed(failure, baseTime.Add(2*time.Minute)); err != nil {
			t.Fatalf("MarkPermanentlyFailed() error = %v", err)
		}
		if m.Status() != message.StatusPermanentlyFailed || !m.Status().Terminal() {
			t.Fatalf("status = %s, want terminal PERMANENTLY_FAILED", m.Status())
		}
	})

	t.Run("unknown final", func(t *testing.T) {
		m := newImmediate(t, 3)
		mustStart(t, m)
		failure := message.Failure{
			Category:  message.FailureSubmissionUnknown,
			Code:      "submission.result_unknown",
			Retryable: true,
		}
		if err := m.MarkSubmissionUnknown(failure, baseTime.Add(2*time.Minute)); err != nil {
			t.Fatalf("MarkSubmissionUnknown() error = %v", err)
		}
		if err := m.MarkUnknownFinal(failure, baseTime.Add(10*time.Minute)); err != nil {
			t.Fatalf("MarkUnknownFinal() error = %v", err)
		}
		if m.Status() != message.StatusUnknownFinal {
			t.Fatalf("status = %s, want UNKNOWN_FINAL", m.Status())
		}
	})
}

func TestSubmissionUnknownRequiresMatchingFailureCategory(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 3)
	mustStart(t, m)
	before := m.Snapshot()
	err := m.MarkSubmissionUnknown(retryableNetworkFailure(), baseTime.Add(2*time.Minute))
	if !errors.Is(err, message.ErrInvalidMessage) {
		t.Fatalf("MarkSubmissionUnknown() error = %v, want invalid message", err)
	}
	if !reflect.DeepEqual(m.Snapshot(), before) {
		t.Fatal("message mutated after invalid unknown failure")
	}
}

func TestApplyDeliveryFactsHandlesDuplicateAndRegression(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 3)
	mustStart(t, m)
	acceptedAt := baseTime.Add(2 * time.Minute)
	accepted := message.DeliveryFact{
		Kind:              message.FactProviderAccepted,
		OccurredAt:        acceptedAt,
		ProviderMessageID: "provider-001",
	}
	result, err := m.ApplyDeliveryFact(accepted)
	if err != nil || result != message.ApplyResultApplied {
		t.Fatalf("provider accepted = (%s, %v)", result, err)
	}
	if m.ProviderAcceptedAt() == nil || !m.ProviderAcceptedAt().Equal(acceptedAt) {
		t.Fatalf("provider accepted time = %v, want %v", m.ProviderAcceptedAt(), acceptedAt)
	}

	sequence := m.LatestSequence()
	result, err = m.ApplyDeliveryFact(accepted)
	if err != nil || result != message.ApplyResultDuplicate || m.LatestSequence() != sequence {
		t.Fatalf("duplicate = (%s, %v), sequence=%d", result, err, m.LatestSequence())
	}

	delivered := message.DeliveryFact{Kind: message.FactDelivered, OccurredAt: baseTime.Add(3 * time.Minute)}
	result, err = m.ApplyDeliveryFact(delivered)
	if err != nil || result != message.ApplyResultApplied || m.Status() != message.StatusDelivered {
		t.Fatalf("delivered = (%s, %v), status=%s", result, err, m.Status())
	}

	sequence = m.LatestSequence()
	result, err = m.ApplyDeliveryFact(accepted)
	if err != nil || result != message.ApplyResultIgnoredRegression || m.LatestSequence() != sequence {
		t.Fatalf("late accepted = (%s, %v), sequence=%d", result, err, m.LatestSequence())
	}

	result, err = m.ApplyDeliveryFact(message.DeliveryFact{Kind: message.FactComplained, OccurredAt: baseTime.Add(4 * time.Minute)})
	if err != nil || result != message.ApplyResultApplied || m.Status() != message.StatusComplained {
		t.Fatalf("complained = (%s, %v), status=%s", result, err, m.Status())
	}
}

func TestDeliveredFactCanResolveSubmissionUnknownDirectly(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 3)
	mustStart(t, m)
	unknown := message.Failure{Category: message.FailureSubmissionUnknown, Code: "submission.result_unknown", Retryable: true}
	if err := m.MarkSubmissionUnknown(unknown, baseTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("MarkSubmissionUnknown() error = %v", err)
	}
	result, err := m.ApplyDeliveryFact(message.DeliveryFact{
		Kind:              message.FactDelivered,
		OccurredAt:        baseTime.Add(3 * time.Minute),
		ProviderMessageID: "provider-001",
	})
	if err != nil || result != message.ApplyResultApplied || m.Status() != message.StatusDelivered {
		t.Fatalf("direct delivered = (%s, %v), status=%s", result, err, m.Status())
	}
	if m.ProviderAcceptedAt() != nil {
		t.Fatalf("provider accepted time should remain unknown, got %v", m.ProviderAcceptedAt())
	}
}

func TestBouncedFactRejectsLaterDeliveredRegression(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 3)
	mustStart(t, m)
	result, err := m.ApplyDeliveryFact(message.DeliveryFact{Kind: message.FactBounced, OccurredAt: baseTime.Add(2 * time.Minute)})
	if err != nil || result != message.ApplyResultApplied || m.Status() != message.StatusBounced {
		t.Fatalf("bounced = (%s, %v), status=%s", result, err, m.Status())
	}
	sequence := m.LatestSequence()
	result, err = m.ApplyDeliveryFact(message.DeliveryFact{Kind: message.FactDelivered, OccurredAt: baseTime.Add(3 * time.Minute)})
	if err != nil || result != message.ApplyResultIgnoredRegression {
		t.Fatalf("late delivered = (%s, %v), want ignored", result, err)
	}
	if m.LatestSequence() != sequence {
		t.Fatal("ignored fact changed sequence")
	}
}

func TestApplyDeliveryFactValidatesInput(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 3)
	mustStart(t, m)
	if _, err := m.ApplyDeliveryFact(message.DeliveryFact{Kind: message.FactDelivered}); !errors.Is(err, message.ErrInvalidDeliveryFact) {
		t.Fatalf("missing time error = %v, want invalid delivery fact", err)
	}
	if _, err := m.ApplyDeliveryFact(message.DeliveryFact{Kind: "UNKNOWN", OccurredAt: baseTime}); !errors.Is(err, message.ErrInvalidDeliveryFact) {
		t.Fatalf("unknown kind error = %v, want invalid delivery fact", err)
	}
}

func TestCancelRejectsUnsafeReasonCodeWithoutMutation(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 3)
	before := m.Snapshot()
	changed, err := m.Cancel("unsafe\nvalue", baseTime.Add(time.Minute))
	if changed || !errors.Is(err, message.ErrInvalidMessage) {
		t.Fatalf("Cancel() = (%t, %v), want invalid message", changed, err)
	}
	if !reflect.DeepEqual(m.Snapshot(), before) {
		t.Fatal("message mutated after invalid reason code")
	}
}

func TestDeliveryFactCannotInventDispatchForScheduledMessage(t *testing.T) {
	t.Parallel()

	m := newScheduled(t, 3)
	before := m.Snapshot()
	result, err := m.ApplyDeliveryFact(message.DeliveryFact{Kind: message.FactDelivered, OccurredAt: baseTime.Add(time.Minute)})
	if result != "" || !errors.Is(err, message.ErrInvalidTransition) {
		t.Fatalf("ApplyDeliveryFact() = (%s, %v), want invalid transition", result, err)
	}
	if !reflect.DeepEqual(m.Snapshot(), before) {
		t.Fatal("message mutated after impossible provider fact")
	}
}

func mustStart(t *testing.T, m *message.Message) {
	t.Helper()
	if err := m.StartSending(m.DispatchGeneration(), baseTime.Add(time.Minute)); err != nil {
		t.Fatalf("StartSending() error = %v", err)
	}
}

func retryableNetworkFailure() message.Failure {
	return message.Failure{
		Category:  message.FailureNetwork,
		Code:      "network.connect_timeout",
		Retryable: true,
	}
}
