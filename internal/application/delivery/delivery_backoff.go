package delivery

import (
	"fmt"
	"math/rand/v2"
	"time"
)

type DeliveryRetryPolicy interface {
	NextDelay(attemptNumber uint32) time.Duration
}

// DeliveryFullJitterBackoff spreads provider retries over
// [1ms, min(Cap, Base*2^(attempt-1))]. A positive lower bound is required
// because the Message state machine forbids retrying at the current instant.
type DeliveryFullJitterBackoff struct {
	base time.Duration
	cap  time.Duration
}

func NewDeliveryFullJitterBackoff(base, cap time.Duration) (*DeliveryFullJitterBackoff, error) {
	if base < time.Millisecond || cap < base || cap > 24*time.Hour {
		return nil, fmt.Errorf(
			"%w: delivery backoff requires 1ms <= base <= cap <= 24h",
			ErrInvalidDispatchWorkerConfig,
		)
	}
	return &DeliveryFullJitterBackoff{base: base, cap: cap}, nil
}

func (b *DeliveryFullJitterBackoff) NextDelay(attemptNumber uint32) time.Duration {
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
	return time.Millisecond + time.Duration(rand.Int64N(int64(maximum-time.Millisecond)+1))
}
