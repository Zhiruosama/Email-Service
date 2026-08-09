package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

var ErrPollerFailure = errors.New("background poller failure")

type batchOperation func(context.Context) (uint32, error)
type fatalErrorClassifier func(error) bool

type poller struct {
	name    string
	config  PollConfig
	logger  *slog.Logger
	runOnce batchOperation
	isFatal fatalErrorClassifier
}

func newPoller(
	name string,
	config PollConfig,
	logger *slog.Logger,
	runOnce batchOperation,
	isFatal fatalErrorClassifier,
) *poller {
	if name == "" || logger == nil || runOnce == nil || isFatal == nil {
		panic("bootstrap: invalid poller dependency")
	}
	return &poller{name: name, config: config, logger: logger, runOnce: runOnce, isFatal: isFatal}
}

func (p *poller) Run(ctx context.Context) error {
	var failureAttempt uint32
	for {
		if ctx.Err() != nil {
			return nil
		}
		processed, err := p.runOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if p.isFatal(err) {
				return fmt.Errorf("%w: %s: %v", ErrPollerFailure, p.name, err)
			}
			failureAttempt++
			delay := exponentialFullJitter(p.config.ErrorBase, p.config.ErrorCap, failureAttempt)
			p.logger.Error(
				"background batch failed; retry scheduled",
				"component", p.name,
				"retry_in", delay,
				"error", err,
			)
			if !waitForContext(ctx, delay) {
				return nil
			}
			continue
		}

		failureAttempt = 0
		if processed > 0 {
			// Drain another bounded batch immediately. RunOnce itself performs I/O,
			// so this does not become a CPU spin while backlog exists.
			continue
		}
		if !waitForContext(ctx, p.config.IdleDelay) {
			return nil
		}
	}
}

func exponentialFullJitter(base, cap time.Duration, attempt uint32) time.Duration {
	maximum := base
	for exponent := uint32(1); exponent < attempt && maximum < cap; exponent++ {
		if maximum > cap/2 {
			maximum = cap
			break
		}
		maximum *= 2
	}
	if maximum > cap {
		maximum = cap
	}
	return time.Duration(rand.Int64N(int64(maximum) + 1))
}

func waitForContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
