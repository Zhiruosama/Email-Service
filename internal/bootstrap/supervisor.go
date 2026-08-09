package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrComponentExited = errors.New("runtime component exited unexpectedly")
	ErrShutdownTimeout = errors.New("runtime shutdown timeout")
)

type runtimeComponent struct {
	name  string
	stage int
	run   func(context.Context) error
}

type componentResult struct {
	name  string
	stage int
	err   error
}

// stagedSupervisor starts every component together. On an external shutdown
// it cancels stages in ascending order; on an unexpected component exit it
// cancels all stages immediately and returns the triggering error.
type stagedSupervisor struct {
	components      []runtimeComponent
	shutdownTimeout time.Duration
	beforeShutdown  func()
}

func (s stagedSupervisor) Run(ctx context.Context) error {
	if len(s.components) == 0 {
		panic("bootstrap: supervisor requires components")
	}
	base := context.WithoutCancel(ctx)
	maxStage := 0
	stageContexts := make(map[int]context.Context)
	stageCancels := make(map[int]context.CancelFunc)
	stageSizes := make(map[int]int)
	for _, component := range s.components {
		if component.name == "" || component.stage < 0 || component.run == nil {
			panic("bootstrap: invalid runtime component")
		}
		if component.stage > maxStage {
			maxStage = component.stage
		}
		stageSizes[component.stage]++
	}
	for stage := 0; stage <= maxStage; stage++ {
		stageContexts[stage], stageCancels[stage] = context.WithCancel(base)
	}
	defer func() {
		for stage := 0; stage <= maxStage; stage++ {
			stageCancels[stage]()
		}
	}()

	results := make(chan componentResult, len(s.components))
	var workers sync.WaitGroup
	for _, component := range s.components {
		workers.Add(1)
		go func(component runtimeComponent) {
			defer workers.Done()
			results <- componentResult{
				name:  component.name,
				stage: component.stage,
				err:   component.run(stageContexts[component.stage]),
			}
		}(component)
	}
	allDone := make(chan struct{})
	go func() {
		workers.Wait()
		close(allDone)
	}()

	var trigger error
	select {
	case <-ctx.Done():
		if s.beforeShutdown != nil {
			s.beforeShutdown()
		}
	case result := <-results:
		trigger = componentExitError(result)
		if s.beforeShutdown != nil {
			s.beforeShutdown()
		}
		for stage := 0; stage <= maxStage; stage++ {
			stageCancels[stage]()
		}
	}

	shutdownTimer := time.NewTimer(s.shutdownTimeout)
	defer shutdownTimer.Stop()
	if trigger == nil {
		completedByStage := make(map[int]int)
		for stage := 0; stage <= maxStage; stage++ {
			stageCancels[stage]()
			for completedByStage[stage] < stageSizes[stage] {
				select {
				case result := <-results:
					completedByStage[result.stage]++
					if result.err != nil && trigger == nil {
						trigger = fmt.Errorf("component %s stopped: %w", result.name, result.err)
					}
				case <-shutdownTimer.C:
					return ErrShutdownTimeout
				}
			}
		}
	} else {
		select {
		case <-allDone:
		case <-shutdownTimer.C:
			return errors.Join(trigger, ErrShutdownTimeout)
		}
	}
	return trigger
}

func componentExitError(result componentResult) error {
	if result.err != nil {
		return fmt.Errorf("component %s stopped: %w", result.name, result.err)
	}
	return fmt.Errorf("%w: %s", ErrComponentExited, result.name)
}
