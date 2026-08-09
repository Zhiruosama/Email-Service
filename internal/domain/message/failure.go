package message

import (
	"fmt"
	"strings"
)

// FailureCategory is independent from provider-specific error codes.
type FailureCategory string

const (
	FailureValidation        FailureCategory = "VALIDATION"
	FailureAuthentication    FailureCategory = "AUTHENTICATION"
	FailureRateLimited       FailureCategory = "RATE_LIMITED"
	FailureRecipientRejected FailureCategory = "RECIPIENT_REJECTED"
	FailureContentRejected   FailureCategory = "CONTENT_REJECTED"
	FailureProviderDown      FailureCategory = "PROVIDER_UNAVAILABLE"
	FailureNetwork           FailureCategory = "NETWORK"
	FailureTimeoutBeforeSend FailureCategory = "TIMEOUT_BEFORE_SEND"
	FailureSubmissionUnknown FailureCategory = "SUBMISSION_UNKNOWN"
	FailureInternal          FailureCategory = "INTERNAL"
)

func (c FailureCategory) Valid() bool {
	switch c {
	case FailureValidation,
		FailureAuthentication,
		FailureRateLimited,
		FailureRecipientRejected,
		FailureContentRejected,
		FailureProviderDown,
		FailureNetwork,
		FailureTimeoutBeforeSend,
		FailureSubmissionUnknown,
		FailureInternal:
		return true
	default:
		return false
	}
}

// Failure is a sanitized domain failure. Code must be stable and must not
// contain provider responses, addresses, content, or credentials.
type Failure struct {
	Category  FailureCategory
	Code      string
	Retryable bool
}

// Validate exposes failure validation at application boundaries. The domain
// transitions call the same rules internally, so provider adapters and
// persistence ports cannot construct a failure the aggregate would reject.
func (f Failure) Validate() error {
	return f.validate()
}

func (f Failure) validate() error {
	if !f.Category.Valid() {
		return fmt.Errorf("%w: invalid failure category %q", ErrInvalidMessage, f.Category)
	}
	if strings.TrimSpace(f.Code) == "" || len(f.Code) > 128 {
		return fmt.Errorf("%w: failure code must contain 1..128 bytes", ErrInvalidMessage)
	}
	return nil
}
