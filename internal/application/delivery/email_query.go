package delivery

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/google/uuid"
)

var ErrInvalidEmailQuery = errors.New("invalid email query")

type GetEmailQuery struct {
	TenantID       string
	MessageID      string
	IdempotencyKey string
}

type EmailQueryService struct {
	messages ports.MessageRepository
}

func NewEmailQueryService(messages ports.MessageRepository) *EmailQueryService {
	if messages == nil {
		panic("delivery: nil message repository")
	}
	return &EmailQueryService{messages: messages}
}

func (s *EmailQueryService) Get(
	ctx context.Context,
	query GetEmailQuery,
) (ports.MessageRecord, error) {
	if _, err := uuid.Parse(query.TenantID); err != nil {
		return ports.MessageRecord{}, fmt.Errorf("%w: tenant identity is invalid", ErrInvalidEmailQuery)
	}
	byID := query.MessageID != ""
	byKey := query.IdempotencyKey != ""
	if byID == byKey {
		return ports.MessageRecord{}, fmt.Errorf("%w: provide exactly one selector", ErrInvalidEmailQuery)
	}

	if byKey {
		if !idempotencyKeyPattern.MatchString(query.IdempotencyKey) {
			return ports.MessageRecord{}, fmt.Errorf("%w: idempotency key has invalid format", ErrInvalidEmailQuery)
		}
		return s.messages.GetByIdempotencyKey(ctx, query.TenantID, query.IdempotencyKey)
	}
	messageID := strings.TrimSpace(query.MessageID)
	if messageID != query.MessageID {
		return ports.MessageRecord{}, fmt.Errorf("%w: message id has surrounding whitespace", ErrInvalidEmailQuery)
	}
	if _, err := uuid.Parse(messageID); err != nil {
		return ports.MessageRecord{}, fmt.Errorf("%w: message id is invalid", ErrInvalidEmailQuery)
	}
	record, err := s.messages.GetByID(ctx, messageID)
	if err != nil {
		return ports.MessageRecord{}, err
	}
	// Cross-tenant IDs deliberately look absent; returning PermissionDenied
	// would disclose that another tenant owns this UUID.
	if record.TenantID != query.TenantID {
		return ports.MessageRecord{}, ports.ErrMessageNotFound
	}
	return record, nil
}
