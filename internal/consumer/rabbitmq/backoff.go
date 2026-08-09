package rabbitmq

import (
	"math/rand/v2"
	"time"
)

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

func boundedJitter(base, cap time.Duration) time.Duration {
	if base == cap {
		return base
	}
	return base + time.Duration(rand.Int64N(int64(cap-base)+1))
}
