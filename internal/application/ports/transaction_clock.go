package ports

import (
	"context"
	"errors"
	"time"
)

var ErrTransactionClock = errors.New("transaction clock failure")

// TransactionClock returns the database transaction timestamp. Business
// deadlines use this shared source instead of individual worker node clocks.
type TransactionClock interface {
	Now(context.Context) (time.Time, error)
}
