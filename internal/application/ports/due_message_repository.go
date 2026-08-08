package ports

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const MaxDueMessageBatchSize uint32 = 1000

var (
	ErrInvalidDueMessageQuery = errors.New("invalid due message query")
	ErrDueMessageRepository   = errors.New("due message repository failure")
)

// DueMessageQuery bounds one short Scheduler transaction. PostgreSQL supplies
// the authoritative time used to decide which rows are due.
type DueMessageQuery struct {
	Limit uint32
}

func (q DueMessageQuery) Validate() error {
	if q.Limit == 0 || q.Limit > MaxDueMessageBatchSize {
		return fmt.Errorf(
			"%w: limit must be in range 1..%d",
			ErrInvalidDueMessageQuery,
			MaxDueMessageBatchSize,
		)
	}
	return nil
}

// DueMessageBatch carries the transaction timestamp used by PostgreSQL's due
// predicate. The application must use EvaluatedAt for every transition in the
// batch so database selection and domain deadlines share one clock.
type DueMessageBatch struct {
	EvaluatedAt time.Time
	Records     []MessageRecord
}

// DueMessageRepository locks due scheduled and retry messages for the current
// transaction. Callers must finish all state changes before that transaction
// commits; the lock is not a lease for external work.
type DueMessageRepository interface {
	LockDue(context.Context, DueMessageQuery) (DueMessageBatch, error)
}
