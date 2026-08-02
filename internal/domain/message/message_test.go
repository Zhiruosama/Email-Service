package message_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

var baseTime = time.Date(2026, time.August, 2, 10, 0, 0, 0, time.UTC)

func TestNewImmediateMessageQueuesAndRequestsDispatch(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 3)
	if got := m.Status(); got != message.StatusQueued {
		t.Fatalf("status = %s, want QUEUED", got)
	}
	if got := m.DispatchGeneration(); got != 1 {
		t.Fatalf("generation = %d, want 1", got)
	}

	events := m.PendingEvents()
	if len(events) != 3 {
		t.Fatalf("event count = %d, want 3", len(events))
	}
	if events[0].Kind != message.EventMessageAccepted || events[0].Sequence != 1 {
		t.Fatalf("first event = %+v, want accepted sequence 1", events[0])
	}
	if events[1].Kind != message.EventStatusChanged || events[1].To != message.StatusQueued || events[1].Sequence != 2 {
		t.Fatalf("second event = %+v, want queued sequence 2", events[1])
	}
	if events[2].Kind != message.EventDispatchRequested || events[2].DispatchGeneration != 1 || events[2].Sequence != 2 {
		t.Fatalf("dispatch event = %+v, want generation 1 at sequence 2", events[2])
	}
}

func TestNewFutureMessageIsScheduledWithoutDispatchEvent(t *testing.T) {
	t.Parallel()

	scheduledAt := baseTime.Add(time.Hour)
	m, err := message.New(message.NewParams{
		ID:               "mail-scheduled",
		Now:              baseTime,
		ScheduledAt:      &scheduledAt,
		DispatchDeadline: baseTime.Add(2 * time.Hour),
		MaxAttempts:      3,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if m.Status() != message.StatusScheduled {
		t.Fatalf("status = %s, want SCHEDULED", m.Status())
	}
	if m.DispatchGeneration() != 0 {
		t.Fatalf("generation = %d, want 0", m.DispatchGeneration())
	}
	for _, event := range m.PendingEvents() {
		if event.Kind == message.EventDispatchRequested {
			t.Fatal("future scheduled message emitted dispatch request")
		}
	}
}

func TestNewRejectsBrokenInvariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params message.NewParams
	}{
		{
			name:   "missing id",
			params: message.NewParams{Now: baseTime, DispatchDeadline: baseTime.Add(time.Hour), MaxAttempts: 1},
		},
		{
			name:   "deadline not after acceptance",
			params: message.NewParams{ID: "mail", Now: baseTime, DispatchDeadline: baseTime, MaxAttempts: 1},
		},
		{
			name:   "zero attempts",
			params: message.NewParams{ID: "mail", Now: baseTime, DispatchDeadline: baseTime.Add(time.Hour)},
		},
		{
			name: "schedule at deadline",
			params: func() message.NewParams {
				deadline := baseTime.Add(time.Hour)
				return message.NewParams{ID: "mail", Now: baseTime, ScheduledAt: &deadline, DispatchDeadline: deadline, MaxAttempts: 1}
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := message.New(test.params); !errors.Is(err, message.ErrInvalidMessage) {
				t.Fatalf("New() error = %v, want ErrInvalidMessage", err)
			}
		})
	}
}

func TestRestoreDoesNotEmitEvents(t *testing.T) {
	t.Parallel()

	original := newImmediate(t, 3)
	restored, err := message.Restore(original.Snapshot())
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if len(restored.PendingEvents()) != 0 {
		t.Fatalf("restored event count = %d, want 0", len(restored.PendingEvents()))
	}
	if !reflect.DeepEqual(restored.Snapshot(), original.Snapshot()) {
		t.Fatalf("restored snapshot differs\n got: %+v\nwant: %+v", restored.Snapshot(), original.Snapshot())
	}
}

func TestRestoreRejectsCorruptSnapshots(t *testing.T) {
	t.Parallel()

	valid := newImmediate(t, 3).Snapshot()
	tests := []struct {
		name   string
		mutate func(*message.Snapshot)
	}{
		{name: "invalid status", mutate: func(s *message.Snapshot) { s.Status = "BROKEN" }},
		{name: "attempts exceed maximum", mutate: func(s *message.Snapshot) { s.AttemptCount = s.MaxAttempts + 1 }},
		{name: "missing dispatch generation", mutate: func(s *message.Snapshot) { s.DispatchGeneration = 0 }},
		{name: "missing sequence", mutate: func(s *message.Snapshot) { s.LatestSequence = 0 }},
		{
			name: "retry status without retry time",
			mutate: func(s *message.Snapshot) {
				s.Status = message.StatusRetryScheduled
				s.AttemptCount = 1
				s.NextAttemptAt = nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			snapshot := valid
			test.mutate(&snapshot)
			if _, err := message.Restore(snapshot); err == nil {
				t.Fatal("Restore() error = nil, want rejection")
			}
		})
	}
}

func TestPullEventsClearsBuffer(t *testing.T) {
	t.Parallel()

	m := newImmediate(t, 3)
	if len(m.PullEvents()) == 0 {
		t.Fatal("PullEvents() returned no events")
	}
	if got := len(m.PendingEvents()); got != 0 {
		t.Fatalf("pending events after pull = %d, want 0", got)
	}
}

func newImmediate(t *testing.T, maxAttempts uint32) *message.Message {
	t.Helper()
	m, err := message.New(message.NewParams{
		ID:               "mail-immediate",
		Now:              baseTime,
		DispatchDeadline: baseTime.Add(time.Hour),
		MaxAttempts:      maxAttempts,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return m
}

func newScheduled(t *testing.T, maxAttempts uint32) *message.Message {
	t.Helper()
	scheduledAt := baseTime.Add(10 * time.Minute)
	m, err := message.New(message.NewParams{
		ID:               "mail-scheduled",
		Now:              baseTime,
		ScheduledAt:      &scheduledAt,
		DispatchDeadline: baseTime.Add(time.Hour),
		MaxAttempts:      maxAttempts,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return m
}
