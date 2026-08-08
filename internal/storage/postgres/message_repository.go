// Package postgres implements application ports with PostgreSQL-specific SQL
// and error handling.
package postgres

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DBTX is intentionally satisfied by both *pgxpool.Pool and pgx.Tx. A
// transaction coordinator can therefore reuse this repository without a
// second implementation.
type DBTX interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type MessageRepository struct {
	db DBTX
}

var _ ports.MessageRepository = (*MessageRepository)(nil)

func NewMessageRepository(db DBTX) *MessageRepository {
	if db == nil {
		panic("postgres: nil DBTX")
	}
	return &MessageRepository{db: db}
}

func (r *MessageRepository) Create(
	ctx context.Context,
	record ports.MessageRecord,
) (ports.CreateMessageResult, error) {
	if err := record.ValidateForCreate(); err != nil {
		return ports.CreateMessageResult{}, err
	}

	arguments, err := createArguments(record)
	if err != nil {
		return ports.CreateMessageResult{}, err
	}
	var persistedVersion int64
	err = r.db.QueryRow(ctx, insertMessageQuery, arguments...).Scan(&persistedVersion)
	if err == nil {
		if persistedVersion != 0 {
			return ports.CreateMessageResult{}, corruptRecordError("created message version is not zero", nil)
		}
		return ports.CreateMessageResult{
			Disposition: ports.CreateDispositionCreated,
			Record:      record,
		}, nil
	}

	if errors.Is(err, pgx.ErrNoRows) {
		existing, findErr := r.GetByIdempotencyKey(ctx, record.TenantID, record.IdempotencyKey)
		if findErr != nil {
			return ports.CreateMessageResult{}, findErr
		}
		if subtle.ConstantTimeCompare(
			existing.PayloadFingerprint[:],
			record.PayloadFingerprint[:],
		) != 1 {
			return ports.CreateMessageResult{}, ports.ErrIdempotencyConflict
		}
		return ports.CreateMessageResult{
			Disposition: ports.CreateDispositionDuplicate,
			Record:      existing,
		}, nil
	}

	var pgError *pgconn.PgError
	if errors.As(err, &pgError) && pgError.ConstraintName == "mail_messages_pkey" {
		return ports.CreateMessageResult{}, ports.ErrMessageIDConflict
	}
	return ports.CreateMessageResult{}, mapRepositoryError(ctx, "create message", err)
}

func (r *MessageRepository) GetByID(
	ctx context.Context,
	messageID string,
) (ports.MessageRecord, error) {
	if _, err := uuid.Parse(messageID); err != nil {
		return ports.MessageRecord{}, fmt.Errorf("%w: message id must be a UUID", ports.ErrInvalidMessageRecord)
	}
	return r.get(ctx, getMessageByIDQuery, messageID)
}

func (r *MessageRepository) GetByIdempotencyKey(
	ctx context.Context,
	tenantID string,
	idempotencyKey string,
) (ports.MessageRecord, error) {
	if _, err := uuid.Parse(tenantID); err != nil {
		return ports.MessageRecord{}, fmt.Errorf("%w: tenant id must be a UUID", ports.ErrInvalidMessageRecord)
	}
	trimmedKey := strings.TrimSpace(idempotencyKey)
	if trimmedKey == "" || trimmedKey != idempotencyKey || len(idempotencyKey) > 255 {
		return ports.MessageRecord{}, fmt.Errorf("%w: idempotency key must contain 1..255 bytes without surrounding whitespace", ports.ErrInvalidMessageRecord)
	}
	return r.get(ctx, getMessageByIdempotencyKeyQuery, tenantID, idempotencyKey)
}

func (r *MessageRepository) get(
	ctx context.Context,
	query string,
	arguments ...any,
) (ports.MessageRecord, error) {
	record, err := scanMessageRecord(r.db.QueryRow(ctx, query, arguments...))
	if err == nil {
		return record, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.MessageRecord{}, ports.ErrMessageNotFound
	}
	var corrupt *corruptMessageRecordError
	if errors.As(err, &corrupt) {
		return ports.MessageRecord{}, err
	}
	return ports.MessageRecord{}, mapRepositoryError(ctx, "read message", err)
}

func (r *MessageRepository) Save(
	ctx context.Context,
	aggregate *message.Message,
) (uint64, error) {
	if aggregate == nil {
		return 0, fmt.Errorf("%w: message is required", ports.ErrInvalidMessageRecord)
	}
	if _, err := uuid.Parse(aggregate.ID()); err != nil {
		return 0, fmt.Errorf("%w: message id must be a UUID", ports.ErrInvalidMessageRecord)
	}

	arguments, err := updateArguments(aggregate.Snapshot())
	if err != nil {
		return 0, err
	}
	var persistedVersion int64
	if err := r.db.QueryRow(ctx, updateMessageQuery, arguments...).Scan(&persistedVersion); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ports.ErrConcurrentUpdate
		}
		return 0, mapRepositoryError(ctx, "save message", err)
	}
	if persistedVersion < 0 {
		return 0, corruptRecordError("negative version returned after save", nil)
	}
	newVersion := uint64(persistedVersion)
	if newVersion != aggregate.Version()+1 {
		return 0, corruptRecordError("unexpected version returned after save", nil)
	}
	return newVersion, nil
}

type repositoryError struct {
	operation string
	cause     error
}

func (e *repositoryError) Error() string {
	return fmt.Sprintf("%s: %s", ports.ErrMessageRepository, e.operation)
}

func (e *repositoryError) Unwrap() error { return ports.ErrMessageRepository }

// Cause is available to internal observability code without exposing the
// driver error through errors.Unwrap or a public API response.
func (e *repositoryError) Cause() error { return e.cause }

func mapRepositoryError(ctx context.Context, operation string, cause error) error {
	if cause == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	return &repositoryError{operation: operation, cause: cause}
}

type corruptMessageRecordError struct {
	detail string
	cause  error
}

func (e *corruptMessageRecordError) Error() string {
	return fmt.Sprintf("%s: %s", ports.ErrCorruptMessageRecord, e.detail)
}

func (e *corruptMessageRecordError) Unwrap() error { return ports.ErrCorruptMessageRecord }

func (e *corruptMessageRecordError) Cause() error { return e.cause }

func corruptRecordError(detail string, cause error) error {
	return &corruptMessageRecordError{detail: detail, cause: cause}
}
