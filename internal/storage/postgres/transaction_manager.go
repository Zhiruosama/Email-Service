package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const rollbackTimeout = 5 * time.Second

type TransactionManager struct {
	pool *pgxpool.Pool
}

var _ ports.Transactor = (*TransactionManager)(nil)

func NewTransactionManager(pool *pgxpool.Pool) *TransactionManager {
	if pool == nil {
		panic("postgres: nil pool")
	}
	return &TransactionManager{pool: pool}
}

func (m *TransactionManager) WithinTransaction(
	ctx context.Context,
	callback ports.TransactionFunc,
) (returnedErr error) {
	if callback == nil {
		return fmt.Errorf("%w: callback is required", ports.ErrTransaction)
	}
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return mapStorageError(ctx, ports.ErrTransaction, "begin", err)
	}
	committed := false
	defer func() {
		if recovered := recover(); recovered != nil {
			rollbackTransaction(tx)
			panic(recovered)
		}
		if committed {
			return
		}
		if rollbackErr := rollbackTransaction(tx); rollbackErr != nil {
			mapped := mapStorageError(context.Background(), ports.ErrTransaction, "rollback", rollbackErr)
			if returnedErr == nil {
				returnedErr = mapped
			} else {
				returnedErr = errors.Join(returnedErr, mapped)
			}
		}
	}()

	unit := &postgresUnitOfWork{
		messages:    NewMessageRepository(tx),
		outbox:      NewOutboxRepository(tx),
		dueMessages: NewDueMessageRepository(tx),
	}
	if err := callback(unit); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return mapStorageError(ctx, ports.ErrTransaction, "commit", err)
	}
	committed = true
	return nil
}

func rollbackTransaction(tx pgx.Tx) error {
	ctx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
	defer cancel()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return err
	}
	return nil
}

type postgresUnitOfWork struct {
	messages    ports.MessageRepository
	outbox      ports.OutboxRepository
	dueMessages ports.DueMessageRepository
}

func (u *postgresUnitOfWork) Messages() ports.MessageRepository { return u.messages }

func (u *postgresUnitOfWork) Outbox() ports.OutboxRepository { return u.outbox }

func (u *postgresUnitOfWork) DueMessages() ports.DueMessageRepository { return u.dueMessages }
