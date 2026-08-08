package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/jackc/pgx/v5"
)

// DueMessageRepository requires a transaction because row locks acquired by
// FOR UPDATE are only useful while that transaction remains open.
type DueMessageRepository struct {
	tx pgx.Tx
}

var _ ports.DueMessageRepository = (*DueMessageRepository)(nil)

func NewDueMessageRepository(tx pgx.Tx) *DueMessageRepository {
	if tx == nil {
		panic("postgres: nil transaction")
	}
	return &DueMessageRepository{tx: tx}
}

func (r *DueMessageRepository) LockDue(
	ctx context.Context,
	query ports.DueMessageQuery,
) (ports.DueMessageBatch, error) {
	if err := query.Validate(); err != nil {
		return ports.DueMessageBatch{}, err
	}

	rows, err := r.tx.Query(ctx, lockDueMessagesQuery, int32(query.Limit))
	if err != nil {
		return ports.DueMessageBatch{}, mapStorageError(
			ctx,
			ports.ErrDueMessageRepository,
			"lock due messages",
			err,
		)
	}
	defer rows.Close()

	records := make([]ports.MessageRecord, 0, query.Limit)
	var evaluatedAt time.Time
	for rows.Next() {
		var rowEvaluatedAt time.Time
		record, scanErr := scanMessageRecord(rows, &rowEvaluatedAt)
		if scanErr != nil {
			var corrupt *corruptMessageRecordError
			if errors.As(scanErr, &corrupt) {
				return ports.DueMessageBatch{}, scanErr
			}
			return ports.DueMessageBatch{}, mapStorageError(
				ctx,
				ports.ErrDueMessageRepository,
				"scan due message",
				scanErr,
			)
		}
		rowEvaluatedAt = rowEvaluatedAt.UTC()
		if rowEvaluatedAt.IsZero() {
			return ports.DueMessageBatch{}, corruptRecordError(
				"database returned an empty Scheduler timestamp",
				nil,
			)
		}
		if evaluatedAt.IsZero() {
			evaluatedAt = rowEvaluatedAt
		} else if !evaluatedAt.Equal(rowEvaluatedAt) {
			return ports.DueMessageBatch{}, corruptRecordError(
				"database returned inconsistent Scheduler timestamps",
				nil,
			)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return ports.DueMessageBatch{}, mapStorageError(
			ctx,
			ports.ErrDueMessageRepository,
			"iterate due messages",
			err,
		)
	}
	return ports.DueMessageBatch{EvaluatedAt: evaluatedAt, Records: records}, nil
}
