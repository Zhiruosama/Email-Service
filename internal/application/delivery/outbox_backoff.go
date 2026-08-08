package delivery

import (
	"fmt"
	"math/rand/v2"
	"time"
)

type OutboxRetryPolicy interface {
	NextDelay(attemptNumber uint32) time.Duration
}

// FullJitterBackoff returns a uniformly random duration in
// [0, min(Cap, Base*2^(attempt-1))].
type FullJitterBackoff struct {
	base time.Duration
	cap  time.Duration
}

func NewFullJitterBackoff(base, cap time.Duration) (*FullJitterBackoff, error) {
	if base <= 0 || cap < base || cap > 24*time.Hour {
		return nil, fmt.Errorf(
			"%w: backoff requires 0 < base <= cap <= 24h",
			ErrInvalidOutboxRelayConfig,
		)
	}
	return &FullJitterBackoff{base: base, cap: cap}, nil
}

func (b *FullJitterBackoff) NextDelay(attemptNumber uint32) time.Duration {
	maximum := b.base
	for exponent := uint32(1); exponent < attemptNumber && maximum < b.cap; exponent++ {
		if maximum > b.cap/2 {
			maximum = b.cap
			break
		}
		maximum *= 2
	}
	if maximum >= b.cap {
		maximum = b.cap
	}
	return time.Duration(rand.Int64N(int64(maximum) + 1))
}
