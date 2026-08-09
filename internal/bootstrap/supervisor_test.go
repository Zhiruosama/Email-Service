package bootstrap

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSupervisorStopsStagesInOrder(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var stopped []string
	component := func(name string, stage int) runtimeComponent {
		return runtimeComponent{
			name:  name,
			stage: stage,
			run: func(ctx context.Context) error {
				<-ctx.Done()
				mu.Lock()
				stopped = append(stopped, name)
				mu.Unlock()
				return nil
			},
		}
	}
	supervisor := stagedSupervisor{
		components: []runtimeComponent{
			component("readiness", 0),
			component("producer-a", 1),
			component("producer-b", 1),
			component("consumer", 2),
			component("grpc", 3),
		},
		shutdownTimeout: time.Second,
	}
	result := make(chan error, 1)
	go func() { result <- supervisor.Run(ctx) }()
	time.Sleep(time.Millisecond)
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("supervisor shutdown: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	positions := make(map[string]int, len(stopped))
	for index, name := range stopped {
		positions[name] = index
	}
	if positions["readiness"] > positions["producer-a"] ||
		positions["readiness"] > positions["producer-b"] ||
		positions["producer-a"] > positions["consumer"] ||
		positions["producer-b"] > positions["consumer"] ||
		positions["consumer"] > positions["grpc"] {
		t.Fatalf("shutdown order = %v", stopped)
	}
}

func TestSupervisorCancelsPeersOnUnexpectedExit(t *testing.T) {
	t.Parallel()
	peerStopped := make(chan struct{})
	supervisor := stagedSupervisor{
		components: []runtimeComponent{
			{name: "failed", stage: 0, run: func(context.Context) error { return errors.New("boom") }},
			{name: "peer", stage: 1, run: func(ctx context.Context) error {
				<-ctx.Done()
				close(peerStopped)
				return nil
			}},
		},
		shutdownTimeout: time.Second,
	}
	err := supervisor.Run(context.Background())
	if err == nil {
		t.Fatal("unexpected component error was suppressed")
	}
	select {
	case <-peerStopped:
	default:
		t.Fatal("peer was not cancelled after component failure")
	}
}

func TestSupervisorBoundsShutdown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	defer close(release)
	supervisor := stagedSupervisor{
		components: []runtimeComponent{{
			name:  "stuck",
			stage: 0,
			run: func(context.Context) error {
				<-release
				return nil
			},
		}},
		shutdownTimeout: 10 * time.Millisecond,
	}
	cancel()
	if err := supervisor.Run(ctx); !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("shutdown error = %v, want ErrShutdownTimeout", err)
	}
}
