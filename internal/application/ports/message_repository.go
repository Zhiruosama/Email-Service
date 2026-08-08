// Package ports defines application-facing contracts without tying callers to
// PostgreSQL, RabbitMQ, gRPC, or a specific provider.
package ports

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"github.com/google/uuid"
)

var (
	ErrMessageNotFound      = errors.New("message not found")
	ErrConcurrentUpdate     = errors.New("concurrent message update")
	ErrIdempotencyConflict  = errors.New("idempotency key reused with different payload")
	ErrMessageIDConflict    = errors.New("message id already exists")
	ErrInvalidMessageRecord = errors.New("invalid message record")
	ErrCorruptMessageRecord = errors.New("corrupt persisted message record")
	ErrMessageRepository    = errors.New("message repository failure")
)

// EmailCategory selects isolation and default delivery policy.
type EmailCategory string

const (
	EmailCategoryCritical      EmailCategory = "CRITICAL"
	EmailCategoryTransactional EmailCategory = "TRANSACTIONAL"
	EmailCategoryNotification  EmailCategory = "NOTIFICATION"
	EmailCategoryBulk          EmailCategory = "BULK"
)

func (c EmailCategory) Valid() bool {
	switch c {
	case EmailCategoryCritical,
		EmailCategoryTransactional,
		EmailCategoryNotification,
		EmailCategoryBulk:
		return true
	default:
		return false
	}
}

// DuplicateRiskPolicy controls ambiguous provider submission outcomes.
type DuplicateRiskPolicy string

const (
	DuplicateRiskAvoidDuplicate DuplicateRiskPolicy = "AVOID_DUPLICATE"
	DuplicateRiskPreferDelivery DuplicateRiskPolicy = "PREFER_DELIVERY"
)

func (p DuplicateRiskPolicy) Valid() bool {
	switch p {
	case DuplicateRiskAvoidDuplicate, DuplicateRiskPreferDelivery:
		return true
	default:
		return false
	}
}

// MessageRecord combines immutable submission identity with the mutable
// lifecycle aggregate. PayloadFingerprint is an HMAC-SHA256, not message body
// content or a plain address hash.
type MessageRecord struct {
	TenantID            string
	IdempotencyKey      string
	PayloadFingerprint  [32]byte
	Category            EmailCategory
	Priority            uint8
	DuplicateRiskPolicy DuplicateRiskPolicy
	Message             *message.Message
}

// Validate protects the repository boundary before PostgreSQL is called.
// PostgreSQL constraints remain the final line of defense.
func (r MessageRecord) Validate() error {
	if _, err := uuid.Parse(r.TenantID); err != nil {
		return fmt.Errorf("%w: tenant id must be a UUID", ErrInvalidMessageRecord)
	}
	key := strings.TrimSpace(r.IdempotencyKey)
	if key == "" || key != r.IdempotencyKey || len(r.IdempotencyKey) > 255 {
		return fmt.Errorf("%w: idempotency key must contain 1..255 bytes without surrounding whitespace", ErrInvalidMessageRecord)
	}
	if !r.Category.Valid() {
		return fmt.Errorf("%w: unknown email category %q", ErrInvalidMessageRecord, r.Category)
	}
	if r.Priority > 9 {
		return fmt.Errorf("%w: priority must be in range 0..9", ErrInvalidMessageRecord)
	}
	if !r.DuplicateRiskPolicy.Valid() {
		return fmt.Errorf("%w: unknown duplicate risk policy %q", ErrInvalidMessageRecord, r.DuplicateRiskPolicy)
	}
	if r.Message == nil {
		return fmt.Errorf("%w: message is required", ErrInvalidMessageRecord)
	}
	if _, err := uuid.Parse(r.Message.ID()); err != nil {
		return fmt.Errorf("%w: message id must be a UUID", ErrInvalidMessageRecord)
	}
	return nil
}

// ValidateForCreate additionally requires the initial optimistic-lock version.
func (r MessageRecord) ValidateForCreate() error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Message.Version() != 0 {
		return fmt.Errorf("%w: new message version must be zero", ErrInvalidMessageRecord)
	}
	return nil
}

type CreateDisposition string

const (
	CreateDispositionCreated   CreateDisposition = "CREATED"
	CreateDispositionDuplicate CreateDisposition = "DUPLICATE"
)

type CreateMessageResult struct {
	Disposition CreateDisposition
	Record      MessageRecord
}

// MessageRepository persists lifecycle state. It never pulls domain events;
// the application transaction coordinator owns that decision.
type MessageRepository interface {
	Create(context.Context, MessageRecord) (CreateMessageResult, error)
	GetByID(context.Context, string) (MessageRecord, error)
	GetByIdempotencyKey(context.Context, string, string) (MessageRecord, error)
	Save(context.Context, *message.Message) (uint64, error)
}
