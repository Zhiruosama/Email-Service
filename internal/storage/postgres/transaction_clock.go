package postgres

import (
	"context"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

type TransactionClock struct {
	db DBTX
}

var _ ports.TransactionClock = (*TransactionClock)(nil)

func NewTransactionClock(db DBTX) *TransactionClock {
	if db == nil {
		panic("postgres: nil DBTX")
	}
	return &TransactionClock{db: db}
}

func (c *TransactionClock) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := c.db.QueryRow(ctx, "SELECT transaction_timestamp()").Scan(&now); err != nil {
		return time.Time{}, mapStorageError(
			ctx,
			ports.ErrTransactionClock,
			"read transaction timestamp",
			err,
		)
	}
	return now.UTC(), nil
}
