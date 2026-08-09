package ports

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"
	"unicode/utf8"
)

const maxMIMEMessageBytes = 2 * 1024 * 1024

var (
	ErrInvalidDeliveryMaterial      = errors.New("invalid delivery material")
	ErrInvalidRenderedEmail         = errors.New("invalid rendered email")
	ErrInvalidSenderIdentity        = errors.New("invalid sender identity")
	ErrDeliveryMaterialBuild        = errors.New("delivery material build failure")
	ErrInvalidDeliveryMaterialError = errors.New("invalid delivery material error")
)

// DeliveryMaterial contains the short-lived plaintext needed by a Provider.
// It must never be persisted, placed in Outbox, or included in logs/errors.
type DeliveryMaterial struct {
	EnvelopeFrom string
	EnvelopeTo   string
	MIMEMessage  []byte
}

func (m DeliveryMaterial) Validate() error {
	if !validBareEmailAddress(m.EnvelopeFrom) || !validBareEmailAddress(m.EnvelopeTo) {
		return fmt.Errorf("%w: envelope addresses are invalid", ErrInvalidDeliveryMaterial)
	}
	if len(m.MIMEMessage) == 0 || len(m.MIMEMessage) > maxMIMEMessageBytes {
		return fmt.Errorf("%w: MIME message must contain 1..%d bytes", ErrInvalidDeliveryMaterial, maxMIMEMessageBytes)
	}
	if !utf8.Valid(m.MIMEMessage) || !bytes.Contains(m.MIMEMessage, []byte("\r\n\r\n")) {
		return fmt.Errorf("%w: MIME message encoding is invalid", ErrInvalidDeliveryMaterial)
	}
	return nil
}

func validBareEmailAddress(value string) bool {
	if value == "" || len(value) > 254 || strings.TrimSpace(value) != value ||
		strings.ContainsAny(value, "\r\n") {
		return false
	}
	parsed, err := mail.ParseAddress(value)
	return err == nil && parsed.Name == "" && parsed.Address == value
}

type DeliveryMaterialBuilder interface {
	Build(context.Context, MessageRecord, StartedDeliveryAttempt) (DeliveryMaterial, error)
}

type RenderDeliveryRequest struct {
	TenantID        string
	TemplateKey     string
	TemplateVersion uint32
	Locale          string
	Variables       json.RawMessage
}

type RenderedEmail struct {
	Subject  string
	TextBody string
	HTMLBody string
}

func (e RenderedEmail) Validate() error {
	if strings.TrimSpace(e.Subject) == "" || len(e.Subject) > 998 ||
		strings.ContainsAny(e.Subject, "\r\n") || !utf8.ValidString(e.Subject) {
		return fmt.Errorf("%w: subject is invalid", ErrInvalidRenderedEmail)
	}
	if strings.TrimSpace(e.TextBody) == "" || strings.TrimSpace(e.HTMLBody) == "" ||
		len(e.TextBody) > 256*1024 || len(e.HTMLBody) > 512*1024 ||
		!utf8.ValidString(e.TextBody) || !utf8.ValidString(e.HTMLBody) {
		return fmt.Errorf("%w: text and HTML bodies are invalid", ErrInvalidRenderedEmail)
	}
	return nil
}

type DeliveryTemplateRenderer interface {
	RenderDelivery(context.Context, RenderDeliveryRequest) (RenderedEmail, error)
}

type SenderIdentity struct {
	Key         string
	Address     string
	DisplayName string
}

func (s SenderIdentity) Validate() error {
	if strings.TrimSpace(s.Key) == "" || len(s.Key) > 64 || !validBareEmailAddress(s.Address) ||
		len(s.DisplayName) > 256 || strings.ContainsAny(s.DisplayName, "\r\n") ||
		!utf8.ValidString(s.DisplayName) {
		return fmt.Errorf("%w: sender identity fields are invalid", ErrInvalidSenderIdentity)
	}
	return nil
}

type SenderIdentityResolver interface {
	ResolveSender(context.Context, string, string) (SenderIdentity, error)
}

type MIMEMessageRequest struct {
	AttemptID            string
	Date                 time.Time
	Sender               SenderIdentity
	RecipientAddress     string
	RecipientDisplayName string
	Content              RenderedEmail
}

type MIMEEncoder interface {
	Encode(MIMEMessageRequest) ([]byte, error)
}

// DeliveryMaterialError exposes only a stable retry decision. The private
// cause can be inspected by internal telemetry without entering an unwrap
// chain that might surface plaintext parser or cryptographic details.
type DeliveryMaterialError struct {
	Code      string
	Retryable bool
	cause     error
}

func NewDeliveryMaterialError(code string, retryable bool, cause error) *DeliveryMaterialError {
	return &DeliveryMaterialError{Code: code, Retryable: retryable, cause: cause}
}

func (e *DeliveryMaterialError) Error() string {
	if e == nil {
		return ErrDeliveryMaterialBuild.Error()
	}
	return fmt.Sprintf("%s: %s", ErrDeliveryMaterialBuild, e.Code)
}

func (e *DeliveryMaterialError) Unwrap() error { return ErrDeliveryMaterialBuild }

func (e *DeliveryMaterialError) Cause() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *DeliveryMaterialError) Validate() error {
	if e == nil || !validStableCode(e.Code) {
		return fmt.Errorf("%w: code must be a stable 1..128 byte identifier", ErrInvalidDeliveryMaterialError)
	}
	return nil
}
