package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

type DeliveryMaterialService struct {
	protector ports.PayloadProtector
	renderer  ports.DeliveryTemplateRenderer
	senders   ports.SenderIdentityResolver
	mime      ports.MIMEEncoder
}

var _ ports.DeliveryMaterialBuilder = (*DeliveryMaterialService)(nil)

func NewDeliveryMaterialService(
	protector ports.PayloadProtector,
	renderer ports.DeliveryTemplateRenderer,
	senders ports.SenderIdentityResolver,
	mimeEncoder ports.MIMEEncoder,
) *DeliveryMaterialService {
	if protector == nil || renderer == nil || senders == nil || mimeEncoder == nil {
		panic("delivery: material service dependencies must not be nil")
	}
	return &DeliveryMaterialService{
		protector: protector,
		renderer:  renderer,
		senders:   senders,
		mime:      mimeEncoder,
	}
}

func (s *DeliveryMaterialService) Build(
	ctx context.Context,
	record ports.MessageRecord,
	attempt ports.StartedDeliveryAttempt,
) (ports.DeliveryMaterial, error) {
	if err := record.Validate(); err != nil || record.Submission == nil {
		return ports.DeliveryMaterial{}, materialBuildError("MATERIAL_RECORD_INVALID", false, err)
	}
	if err := attempt.Validate(); err != nil || attempt.MessageID != record.Message.ID() ||
		attempt.AttemptNumber != record.Message.AttemptCount() ||
		attempt.DispatchGeneration != record.Message.DispatchGeneration() {
		return ports.DeliveryMaterial{}, materialBuildError("MATERIAL_ATTEMPT_INVALID", false, err)
	}
	protected := ports.ProtectedPayload{
		KeyID:      record.Submission.PayloadKeyID,
		Ciphertext: record.Submission.EncryptedPayload,
	}
	plaintext, err := s.protector.Open(
		ctx,
		record.TenantID+"/"+record.Message.ID(),
		protected,
	)
	if err != nil {
		return ports.DeliveryMaterial{}, classifyPayloadOpenError(err)
	}
	defer clear(plaintext)

	payload, err := decodeCanonicalPayload(plaintext)
	if err != nil {
		return ports.DeliveryMaterial{}, materialBuildError("PAYLOAD_SCHEMA_INVALID", false, err)
	}
	if err := validateDeliveryPayload(record, payload); err != nil {
		return ports.DeliveryMaterial{}, materialBuildError("PAYLOAD_IDENTITY_MISMATCH", false, err)
	}
	rendered, err := s.renderer.RenderDelivery(ctx, ports.RenderDeliveryRequest{
		TenantID:        record.TenantID,
		TemplateKey:     payload.TemplateKey,
		TemplateVersion: payload.TemplateVersion,
		Locale:          payload.Locale,
		Variables:       append(json.RawMessage(nil), payload.Variables...),
	})
	if err != nil {
		return ports.DeliveryMaterial{}, classifyTemplateRenderError(err)
	}
	if err := rendered.Validate(); err != nil {
		return ports.DeliveryMaterial{}, materialBuildError("RENDERED_EMAIL_INVALID", false, err)
	}
	sender, err := s.senders.ResolveSender(ctx, record.TenantID, payload.SenderIdentityKey)
	if err != nil {
		retryable := !errors.Is(err, ports.ErrSenderIdentityNotAllowed) &&
			!errors.Is(err, ports.ErrInvalidSenderIdentity)
		return ports.DeliveryMaterial{}, materialBuildError("SENDER_IDENTITY_UNAVAILABLE", retryable, err)
	}
	encoded, err := s.mime.Encode(ports.MIMEMessageRequest{
		AttemptID:            attempt.ID,
		Date:                 attempt.StartedAt,
		Sender:               sender,
		RecipientAddress:     payload.RecipientEmail,
		RecipientDisplayName: payload.RecipientDisplayName,
		Content:              rendered,
	})
	if err != nil {
		retryable := !errors.Is(err, ports.ErrInvalidDeliveryMaterial) &&
			!errors.Is(err, ports.ErrInvalidRenderedEmail) &&
			!errors.Is(err, ports.ErrInvalidSenderIdentity)
		return ports.DeliveryMaterial{}, materialBuildError("MIME_BUILD_FAILED", retryable, err)
	}
	material := ports.DeliveryMaterial{
		EnvelopeFrom: sender.Address,
		EnvelopeTo:   payload.RecipientEmail,
		MIMEMessage:  encoded,
	}
	if err := material.Validate(); err != nil {
		clear(encoded)
		return ports.DeliveryMaterial{}, materialBuildError("MATERIAL_OUTPUT_INVALID", false, err)
	}
	return material, nil
}

func decodeCanonicalPayload(plaintext []byte) (canonicalPayload, error) {
	decoder := json.NewDecoder(bytes.NewReader(plaintext))
	decoder.DisallowUnknownFields()
	var payload canonicalPayload
	if err := decoder.Decode(&payload); err != nil {
		return canonicalPayload{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return canonicalPayload{}, errors.New("payload has trailing data")
	}
	return payload, nil
}

func validateDeliveryPayload(record ports.MessageRecord, payload canonicalPayload) error {
	normalizedRecipient, err := normalizeEmail(payload.RecipientEmail)
	if err != nil || normalizedRecipient != payload.RecipientEmail {
		return errors.New("recipient is not normalized")
	}
	if len(payload.RecipientDisplayName) > 512 || strings.ContainsAny(payload.RecipientDisplayName, "\r\n") ||
		!utf8.ValidString(payload.RecipientDisplayName) {
		return errors.New("recipient display name is invalid")
	}
	if payload.SenderIdentityKey != record.Submission.SenderIdentityKey ||
		payload.TemplateKey != record.Submission.TemplateKey ||
		payload.TemplateVersion != record.Submission.TemplateVersion ||
		payload.Locale != record.Submission.Locale ||
		maskEmail(payload.RecipientEmail) != record.Submission.RecipientMasked ||
		payload.Category != record.Category ||
		payload.Priority != record.Priority ||
		payload.DuplicateRiskPolicy != record.DuplicateRiskPolicy ||
		!payload.DispatchDeadline.Equal(record.Message.DispatchDeadline()) ||
		!sameOptionalTime(payload.ScheduledAt, record.Message.ScheduledAt()) {
		return errors.New("encrypted payload does not match persisted safe fields")
	}
	if _, err := canonicalJSONObject(payload.Variables, maxVariablesBytes, maxJSONDepth, "template variables"); err != nil {
		return errors.New("template variables are invalid")
	}
	if _, err := canonicalJSONObject(payload.Metadata, 4*1024, maxJSONDepth, "metadata"); err != nil {
		return errors.New("metadata is invalid")
	}
	if !bytes.Equal(payload.Metadata, record.Submission.Metadata) {
		return errors.New("encrypted metadata does not match persisted safe metadata")
	}
	return nil
}

func sameOptionalTime(first, second *time.Time) bool {
	if first == nil || second == nil {
		return first == nil && second == nil
	}
	return first.Equal(*second)
}

func classifyPayloadOpenError(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return materialBuildError("PAYLOAD_OPEN_TIMEOUT", true, err)
	case errors.Is(err, context.Canceled):
		return materialBuildError("PAYLOAD_OPEN_CANCELED", true, err)
	case errors.Is(err, ports.ErrPayloadKeyUnavailable):
		return materialBuildError("PAYLOAD_KEY_UNAVAILABLE", true, err)
	case errors.Is(err, ports.ErrPayloadAuthentication),
		errors.Is(err, ports.ErrInvalidProtectedPayload):
		return materialBuildError("PAYLOAD_AUTHENTICATION_FAILED", false, err)
	default:
		return materialBuildError("PAYLOAD_OPEN_INTERNAL", true, err)
	}
}

func classifyTemplateRenderError(err error) error {
	retryable := true
	switch {
	case errors.Is(err, ports.ErrTemplateNotFound),
		errors.Is(err, ports.ErrTemplateNotAllowed),
		errors.Is(err, ports.ErrTemplateVariables),
		errors.Is(err, ports.ErrInvalidRenderedEmail):
		retryable = false
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		retryable = true
	}
	return materialBuildError("TEMPLATE_RENDER_FAILED", retryable, err)
}

func materialBuildError(code string, retryable bool, cause error) error {
	return ports.NewDeliveryMaterialError(code, retryable, cause)
}
