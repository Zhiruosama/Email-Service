package ports

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"github.com/google/uuid"
)

var (
	ErrInvalidProviderRequest = errors.New("invalid provider request")
	ErrInvalidProviderResult  = errors.New("invalid provider result")
)

type ProviderRequest struct {
	AttemptID           string
	MessageID           string
	TenantID            string
	AttemptNumber       uint32
	DispatchGeneration  uint64
	Category            EmailCategory
	DuplicateRiskPolicy DuplicateRiskPolicy
}

func (r ProviderRequest) Validate() error {
	if _, err := uuid.Parse(r.AttemptID); err != nil {
		return fmt.Errorf("%w: attempt id must be a UUID", ErrInvalidProviderRequest)
	}
	if _, err := uuid.Parse(r.MessageID); err != nil {
		return fmt.Errorf("%w: message id must be a UUID", ErrInvalidProviderRequest)
	}
	if _, err := uuid.Parse(r.TenantID); err != nil {
		return fmt.Errorf("%w: tenant id must be a UUID", ErrInvalidProviderRequest)
	}
	if r.AttemptNumber == 0 || r.AttemptNumber > math.MaxInt32 {
		return fmt.Errorf("%w: attempt number must fit a positive PostgreSQL INTEGER", ErrInvalidProviderRequest)
	}
	if r.DispatchGeneration == 0 || r.DispatchGeneration > math.MaxInt64 {
		return fmt.Errorf("%w: dispatch generation must fit a positive PostgreSQL BIGINT", ErrInvalidProviderRequest)
	}
	if !r.Category.Valid() {
		return fmt.Errorf("%w: unknown email category %q", ErrInvalidProviderRequest, r.Category)
	}
	if !r.DuplicateRiskPolicy.Valid() {
		return fmt.Errorf("%w: unknown duplicate risk policy %q", ErrInvalidProviderRequest, r.DuplicateRiskPolicy)
	}
	return nil
}

type ProviderOutcome string

const (
	ProviderOutcomeAccepted          ProviderOutcome = "ACCEPTED"
	ProviderOutcomeFailed            ProviderOutcome = "FAILED"
	ProviderOutcomeSubmissionUnknown ProviderOutcome = "SUBMISSION_UNKNOWN"
)

type ProviderResult struct {
	Outcome           ProviderOutcome
	ProviderMessageID string
	Failure           *message.Failure
}

func (r ProviderResult) Validate() error {
	switch r.Outcome {
	case ProviderOutcomeAccepted:
		trimmed := strings.TrimSpace(r.ProviderMessageID)
		if trimmed == "" || trimmed != r.ProviderMessageID || len(r.ProviderMessageID) > 512 {
			return fmt.Errorf("%w: accepted result requires a 1..512 byte provider message id", ErrInvalidProviderResult)
		}
		if r.Failure != nil {
			return fmt.Errorf("%w: accepted result cannot contain a failure", ErrInvalidProviderResult)
		}
	case ProviderOutcomeFailed:
		if r.ProviderMessageID != "" || r.Failure == nil {
			return fmt.Errorf("%w: failed result requires only failure information", ErrInvalidProviderResult)
		}
		if err := r.Failure.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidProviderResult, err)
		}
		if r.Failure.Category == message.FailureSubmissionUnknown {
			return fmt.Errorf("%w: ambiguous failure requires submission-unknown outcome", ErrInvalidProviderResult)
		}
	case ProviderOutcomeSubmissionUnknown:
		if r.ProviderMessageID != "" || r.Failure == nil {
			return fmt.Errorf("%w: submission-unknown result requires only failure information", ErrInvalidProviderResult)
		}
		if err := r.Failure.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidProviderResult, err)
		}
		if r.Failure.Category != message.FailureSubmissionUnknown {
			return fmt.Errorf("%w: submission-unknown outcome requires matching failure category", ErrInvalidProviderResult)
		}
	default:
		return fmt.Errorf("%w: unknown outcome %q", ErrInvalidProviderResult, r.Outcome)
	}
	return nil
}

// EmailProvider returns a normalized observation for every external outcome.
// Transport errors are data here: an adapter must distinguish a known failure
// from an ambiguous submission instead of collapsing both into a Go error.
type EmailProvider interface {
	Key() string
	Submit(context.Context, ProviderRequest) ProviderResult
}
