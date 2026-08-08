package postgres

import (
	"fmt"
	"math"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func createArguments(record ports.MessageRecord) ([]any, error) {
	snapshot := record.Message.Snapshot()
	counters, err := databaseCounters(snapshot)
	if err != nil {
		return nil, err
	}
	failureCategory, failureCode, failureRetryable := failureArguments(snapshot.LastFailure)

	return []any{
		snapshot.ID,
		record.TenantID,
		record.IdempotencyKey,
		record.PayloadFingerprint[:],
		string(record.Category),
		int16(record.Priority),
		string(record.DuplicateRiskPolicy),
		string(snapshot.Status),
		snapshot.ScheduledAt,
		snapshot.DispatchDeadline,
		snapshot.NextAttemptAt,
		counters.dispatchGeneration,
		counters.attemptCount,
		counters.maxAttempts,
		snapshot.ProviderAcceptedAt,
		nullableString(snapshot.ProviderMessageID),
		counters.latestSequence,
		counters.version,
		failureCategory,
		failureCode,
		failureRetryable,
		snapshot.AcceptedAt,
		snapshot.UpdatedAt,
	}, nil
}

func updateArguments(snapshot message.Snapshot) ([]any, error) {
	if snapshot.Version >= math.MaxInt64 {
		return nil, fmt.Errorf(
			"%w: message version cannot be incremented in PostgreSQL",
			ports.ErrInvalidMessageRecord,
		)
	}
	counters, err := databaseCounters(snapshot)
	if err != nil {
		return nil, err
	}
	failureCategory, failureCode, failureRetryable := failureArguments(snapshot.LastFailure)

	return []any{
		string(snapshot.Status),
		snapshot.ScheduledAt,
		snapshot.DispatchDeadline,
		snapshot.NextAttemptAt,
		counters.dispatchGeneration,
		counters.attemptCount,
		counters.maxAttempts,
		snapshot.ProviderAcceptedAt,
		nullableString(snapshot.ProviderMessageID),
		counters.latestSequence,
		failureCategory,
		failureCode,
		failureRetryable,
		snapshot.UpdatedAt,
		snapshot.ID,
		counters.version,
	}, nil
}

type persistedCounters struct {
	dispatchGeneration int64
	attemptCount       int32
	maxAttempts        int32
	latestSequence     int64
	version            int64
}

func databaseCounters(snapshot message.Snapshot) (persistedCounters, error) {
	if snapshot.DispatchGeneration > math.MaxInt64 ||
		snapshot.LatestSequence > math.MaxInt64 ||
		snapshot.Version > math.MaxInt64 ||
		snapshot.AttemptCount > math.MaxInt32 ||
		snapshot.MaxAttempts > math.MaxInt32 {
		return persistedCounters{}, fmt.Errorf(
			"%w: message counters exceed PostgreSQL integer range",
			ports.ErrInvalidMessageRecord,
		)
	}
	return persistedCounters{
		dispatchGeneration: int64(snapshot.DispatchGeneration),
		attemptCount:       int32(snapshot.AttemptCount),
		maxAttempts:        int32(snapshot.MaxAttempts),
		latestSequence:     int64(snapshot.LatestSequence),
		version:            int64(snapshot.Version),
	}, nil
}

func failureArguments(failure *message.Failure) (any, any, any) {
	if failure == nil {
		return nil, nil, nil
	}
	return string(failure.Category), failure.Code, failure.Retryable
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func scanMessageRecord(row pgx.Row) (ports.MessageRecord, error) {
	var (
		messageID           string
		tenantID            string
		idempotencyKey      string
		fingerprint         []byte
		category            string
		priority            int16
		duplicateRiskPolicy string
		status              string
		scheduledAt         pgtype.Timestamptz
		dispatchDeadline    time.Time
		nextAttemptAt       pgtype.Timestamptz
		dispatchGeneration  int64
		attemptCount        int32
		maxAttempts         int32
		providerAcceptedAt  pgtype.Timestamptz
		providerMessageID   pgtype.Text
		latestSequence      int64
		version             int64
		lastErrorCategory   pgtype.Text
		lastErrorCode       pgtype.Text
		lastErrorRetryable  pgtype.Bool
		acceptedAt          time.Time
		updatedAt           time.Time
	)

	if err := row.Scan(
		&messageID,
		&tenantID,
		&idempotencyKey,
		&fingerprint,
		&category,
		&priority,
		&duplicateRiskPolicy,
		&status,
		&scheduledAt,
		&dispatchDeadline,
		&nextAttemptAt,
		&dispatchGeneration,
		&attemptCount,
		&maxAttempts,
		&providerAcceptedAt,
		&providerMessageID,
		&latestSequence,
		&version,
		&lastErrorCategory,
		&lastErrorCode,
		&lastErrorRetryable,
		&acceptedAt,
		&updatedAt,
	); err != nil {
		return ports.MessageRecord{}, err
	}

	if len(fingerprint) != 32 {
		return ports.MessageRecord{}, corruptRecordError("payload fingerprint is not 32 bytes", nil)
	}
	if priority < 0 || priority > 9 ||
		dispatchGeneration < 0 || attemptCount < 0 || maxAttempts < 0 ||
		latestSequence < 0 || version < 0 {
		return ports.MessageRecord{}, corruptRecordError("persisted numeric value is outside domain range", nil)
	}

	lastFailure, err := scanFailure(lastErrorCategory, lastErrorCode, lastErrorRetryable)
	if err != nil {
		return ports.MessageRecord{}, err
	}
	snapshot := message.Snapshot{
		ID:                 messageID,
		Status:             message.Status(status),
		ScheduledAt:        timestampPointer(scheduledAt),
		DispatchDeadline:   dispatchDeadline,
		NextAttemptAt:      timestampPointer(nextAttemptAt),
		AttemptCount:       uint32(attemptCount),
		MaxAttempts:        uint32(maxAttempts),
		DispatchGeneration: uint64(dispatchGeneration),
		ProviderAcceptedAt: timestampPointer(providerAcceptedAt),
		ProviderMessageID:  textValue(providerMessageID),
		LatestSequence:     uint64(latestSequence),
		Version:            uint64(version),
		AcceptedAt:         acceptedAt,
		UpdatedAt:          updatedAt,
		LastFailure:        lastFailure,
	}
	aggregate, err := message.Restore(snapshot)
	if err != nil {
		return ports.MessageRecord{}, corruptRecordError("domain rejected persisted snapshot", err)
	}

	record := ports.MessageRecord{
		TenantID:            tenantID,
		IdempotencyKey:      idempotencyKey,
		Category:            ports.EmailCategory(category),
		Priority:            uint8(priority),
		DuplicateRiskPolicy: ports.DuplicateRiskPolicy(duplicateRiskPolicy),
		Message:             aggregate,
	}
	copy(record.PayloadFingerprint[:], fingerprint)
	if err := record.Validate(); err != nil {
		return ports.MessageRecord{}, corruptRecordError("persisted metadata is invalid", err)
	}
	return record, nil
}

func scanFailure(category, code pgtype.Text, retryable pgtype.Bool) (*message.Failure, error) {
	if !category.Valid && !code.Valid && !retryable.Valid {
		return nil, nil
	}
	if !category.Valid || !code.Valid || !retryable.Valid {
		return nil, corruptRecordError("failure fields are only partially present", nil)
	}
	return &message.Failure{
		Category:  message.FailureCategory(category.String),
		Code:      code.String,
		Retryable: retryable.Bool,
	}, nil
}

func timestampPointer(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	timestamp := value.Time.UTC()
	return &timestamp
}

func textValue(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}
	return value.String
}
