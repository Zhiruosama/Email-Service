package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestPollerDrainsWorkAndWaitsWhenEmpty(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	runner := newPoller(
		"test",
		PollConfig{IdleDelay: 20 * time.Millisecond, ErrorBase: time.Millisecond, ErrorCap: time.Millisecond},
		discardLogger(),
		func(context.Context) (uint32, error) {
			call := calls.Add(1)
			if call <= 2 {
				return 1, nil
			}
			cancel()
			return 0, nil
		},
		func(error) bool { return false },
	)
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("run poller: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", calls.Load())
	}
}

func TestPollerRetriesTransientAndReturnsFatalError(t *testing.T) {
	t.Parallel()
	transient := errors.New("temporary database failure")
	fatal := errors.New("corrupt record")
	var calls atomic.Int32
	runner := newPoller(
		"test",
		PollConfig{IdleDelay: time.Millisecond, ErrorBase: time.Millisecond, ErrorCap: time.Millisecond},
		discardLogger(),
		func(context.Context) (uint32, error) {
			if calls.Add(1) == 1 {
				return 0, transient
			}
			return 0, fatal
		},
		func(err error) bool { return errors.Is(err, fatal) },
	)
	if err := runner.Run(context.Background()); !errors.Is(err, ErrPollerFailure) {
		t.Fatalf("run error = %v, want ErrPollerFailure", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
}

func TestPollerCancellationInterruptsLongDelay(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	runner := newPoller(
		"test",
		PollConfig{IdleDelay: time.Minute, ErrorBase: time.Minute, ErrorCap: time.Minute},
		discardLogger(),
		func(context.Context) (uint32, error) { return 0, nil },
		func(error) bool { return false },
	)
	result := make(chan error, 1)
	go func() { result <- runner.Run(ctx) }()
	time.Sleep(time.Millisecond)
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("cancelled poller error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("poller did not observe cancellation")
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
