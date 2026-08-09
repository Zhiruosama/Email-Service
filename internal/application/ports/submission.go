package ports

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrTemplateNotFound         = errors.New("template not found")
	ErrTemplateNotAllowed       = errors.New("template not allowed")
	ErrTemplateVariables        = errors.New("template variables rejected")
	ErrSenderIdentityNotAllowed = errors.New("sender identity not allowed")
	ErrPayloadProtection        = errors.New("payload protection failure")
	ErrPayloadKeyUnavailable    = errors.New("payload key is unavailable")
	ErrPayloadAuthentication    = errors.New("payload authentication failed")
	ErrInvalidProtectedPayload  = errors.New("invalid protected payload")
)

type ResolveTemplateRequest struct {
	TenantID         string
	TemplateKey      string
	RequestedVersion *uint32
	Locale           string
	Variables        json.RawMessage
}

type ResolvedTemplate struct {
	Key       string
	Version   uint32
	Locale    string
	Variables json.RawMessage
}

// TemplateResolver is a control-plane read port. Resolving an omitted version
// must pin one immutable published version before message acceptance.
type TemplateResolver interface {
	Resolve(context.Context, ResolveTemplateRequest) (ResolvedTemplate, error)
}

// SubmissionCatalog is the acceptance-time control-plane view. Both template
// and sender authorization must use the authenticated tenant identity.
type SubmissionCatalog interface {
	TemplateResolver
	AuthorizeSender(context.Context, string, string) error
}

type ProtectedPayload struct {
	KeyID      string
	Ciphertext []byte
}

func (p ProtectedPayload) Validate() error {
	if strings.TrimSpace(p.KeyID) == "" || len(p.KeyID) > 128 {
		return fmt.Errorf("%w: key id must contain 1..128 bytes", ErrInvalidProtectedPayload)
	}
	if len(p.Ciphertext) < 29 {
		return fmt.Errorf("%w: ciphertext is too short", ErrInvalidProtectedPayload)
	}
	return nil
}

// PayloadProtector provides two separate properties: randomized authenticated
// encryption for storage and deterministic keyed fingerprints for idempotency.
type PayloadProtector interface {
	Fingerprint([]byte) [32]byte
	Seal(context.Context, string, []byte) (ProtectedPayload, error)
	Open(context.Context, string, ProtectedPayload) ([]byte, error)
}
