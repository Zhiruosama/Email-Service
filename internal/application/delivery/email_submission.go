package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"github.com/google/uuid"
	"golang.org/x/net/idna"
	"golang.org/x/text/language"
)

const (
	maxScheduledWindow = 365 * 24 * time.Hour
	maxVariablesBytes  = 16 * 1024
	maxJSONDepth       = 8
	maxMetadataEntries = 16
)

var (
	ErrInvalidSubmission   = errors.New("invalid email submission")
	ErrSubmissionInvariant = errors.New("email submission invariant violation")
	idempotencyKeyPattern  = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	registeredKeyPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	localePattern          = regexp.MustCompile(`^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$`)
)

type SubmitEmailCommand struct {
	TenantID             string
	IdempotencyKey       string
	RecipientEmail       string
	RecipientDisplayName string
	SenderIdentityKey    string
	TemplateKey          string
	TemplateVersion      *uint32
	Locale               string
	Variables            json.RawMessage
	Category             ports.EmailCategory
	Priority             uint8
	ScheduledAt          *time.Time
	DispatchDeadline     time.Time
	DuplicateRiskPolicy  ports.DuplicateRiskPolicy
	Metadata             map[string]string
}

type SubmitEmailDisposition string

const (
	SubmitEmailAccepted  SubmitEmailDisposition = "ACCEPTED"
	SubmitEmailDuplicate SubmitEmailDisposition = "DUPLICATE"
)

type SubmitEmailResult struct {
	Disposition SubmitEmailDisposition
	Record      ports.MessageRecord
}

type EmailSubmissionService struct {
	transactor ports.Transactor
	catalog    ports.SubmissionCatalog
	protector  ports.PayloadProtector
}

func NewEmailSubmissionService(
	transactor ports.Transactor,
	catalog ports.SubmissionCatalog,
	protector ports.PayloadProtector,
) *EmailSubmissionService {
	if transactor == nil || catalog == nil || protector == nil {
		panic("delivery: submission service dependencies must not be nil")
	}
	return &EmailSubmissionService{
		transactor: transactor,
		catalog:    catalog,
		protector:  protector,
	}
}

func (s *EmailSubmissionService) Submit(
	ctx context.Context,
	command SubmitEmailCommand,
) (SubmitEmailResult, error) {
	normalized, metadataJSON, err := normalizeSubmission(command)
	if err != nil {
		return SubmitEmailResult{}, err
	}
	if err := s.catalog.AuthorizeSender(ctx, normalized.TenantID, normalized.SenderIdentityKey); err != nil {
		return SubmitEmailResult{}, err
	}
	resolved, err := s.catalog.Resolve(ctx, ports.ResolveTemplateRequest{
		TenantID:         normalized.TenantID,
		TemplateKey:      normalized.TemplateKey,
		RequestedVersion: cloneUint32(normalized.TemplateVersion),
		Locale:           normalized.Locale,
		Variables:        append(json.RawMessage(nil), normalized.Variables...),
	})
	if err != nil {
		return SubmitEmailResult{}, err
	}
	if err := validateResolvedTemplate(normalized, resolved); err != nil {
		return SubmitEmailResult{}, err
	}

	canonical, err := canonicalSubmissionPayload(normalized, resolved, metadataJSON)
	if err != nil {
		return SubmitEmailResult{}, err
	}
	messageID := uuid.NewString()
	protected, err := s.protector.Seal(
		ctx,
		normalized.TenantID+"/"+messageID,
		canonical,
	)
	if err != nil {
		return SubmitEmailResult{}, err
	}
	if err := protected.Validate(); err != nil {
		return SubmitEmailResult{}, fmt.Errorf("%w: protector returned invalid data", ErrSubmissionInvariant)
	}

	fingerprint := s.protector.Fingerprint(canonical)
	var (
		result  SubmitEmailResult
		created *message.Message
	)
	err = s.transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
		now, clockErr := unit.Clock().Now(ctx)
		if clockErr != nil {
			return clockErr
		}
		if deadlineErr := validateSubmissionTimes(now, normalized.ScheduledAt, normalized.DispatchDeadline); deadlineErr != nil {
			return deadlineErr
		}
		aggregate, newErr := message.New(message.NewParams{
			ID:               messageID,
			Now:              now,
			ScheduledAt:      normalized.ScheduledAt,
			DispatchDeadline: normalized.DispatchDeadline,
			MaxAttempts:      maxAttemptsFor(normalized.Category),
		})
		if newErr != nil {
			return fmt.Errorf("%w: construct message", ErrSubmissionInvariant)
		}
		record := ports.MessageRecord{
			TenantID:            normalized.TenantID,
			IdempotencyKey:      normalized.IdempotencyKey,
			PayloadFingerprint:  fingerprint,
			Category:            normalized.Category,
			Priority:            normalized.Priority,
			DuplicateRiskPolicy: normalized.DuplicateRiskPolicy,
			Submission: &ports.SubmissionDetails{
				SenderIdentityKey: normalized.SenderIdentityKey,
				TemplateKey:       resolved.Key,
				TemplateVersion:   resolved.Version,
				Locale:            resolved.Locale,
				RecipientMasked:   maskEmail(normalized.RecipientEmail),
				PayloadKeyID:      protected.KeyID,
				EncryptedPayload:  append([]byte(nil), protected.Ciphertext...),
				Metadata:          append(json.RawMessage(nil), metadataJSON...),
			},
			Message: aggregate,
		}
		persisted, createErr := unit.Messages().Create(ctx, record)
		if createErr != nil {
			return createErr
		}
		if persisted.Disposition == ports.CreateDispositionDuplicate {
			result = SubmitEmailResult{Disposition: SubmitEmailDuplicate, Record: persisted.Record}
			return nil
		}
		mapped, mapErr := mapAllMessageEvents(record, aggregate.PendingEvents())
		if mapErr != nil {
			return mapErr
		}
		if appendErr := unit.DeliveryEvents().Append(ctx, mapped.Delivery); appendErr != nil {
			return appendErr
		}
		if appendErr := unit.Outbox().Append(ctx, mapped.Outbox); appendErr != nil {
			return appendErr
		}
		created = aggregate
		result = SubmitEmailResult{Disposition: SubmitEmailAccepted, Record: record}
		return nil
	})
	if err != nil {
		return SubmitEmailResult{}, err
	}
	if created != nil {
		created.PullEvents()
	}
	return result, nil
}

func normalizeSubmission(command SubmitEmailCommand) (SubmitEmailCommand, json.RawMessage, error) {
	if _, err := uuid.Parse(command.TenantID); err != nil {
		return SubmitEmailCommand{}, nil, invalidSubmission("tenant identity is invalid")
	}
	if !idempotencyKeyPattern.MatchString(command.IdempotencyKey) {
		return SubmitEmailCommand{}, nil, invalidSubmission("idempotency key has invalid format")
	}
	email, err := normalizeEmail(command.RecipientEmail)
	if err != nil {
		return SubmitEmailCommand{}, nil, err
	}
	if utf8.RuneCountInString(command.RecipientDisplayName) > 128 || strings.ContainsAny(command.RecipientDisplayName, "\r\n") {
		return SubmitEmailCommand{}, nil, invalidSubmission("recipient display name is invalid")
	}
	if !validRegisteredKey(command.SenderIdentityKey, 64) {
		return SubmitEmailCommand{}, nil, invalidSubmission("sender identity key has invalid format")
	}
	if !validRegisteredKey(command.TemplateKey, 128) {
		return SubmitEmailCommand{}, nil, invalidSubmission("template key has invalid format")
	}
	if command.TemplateVersion != nil && *command.TemplateVersion == 0 {
		return SubmitEmailCommand{}, nil, invalidSubmission("template version must be positive")
	}
	locale, err := language.Parse(command.Locale)
	if err != nil || !localePattern.MatchString(command.Locale) || len(command.Locale) > 35 {
		return SubmitEmailCommand{}, nil, invalidSubmission("locale must be a valid BCP 47 tag")
	}
	if !command.Category.Valid() || command.Priority > 9 || !command.DuplicateRiskPolicy.Valid() {
		return SubmitEmailCommand{}, nil, invalidSubmission("delivery policy is invalid")
	}
	if command.DispatchDeadline.IsZero() {
		return SubmitEmailCommand{}, nil, invalidSubmission("dispatch deadline is required")
	}
	variables, err := canonicalJSONObject(command.Variables, maxVariablesBytes, maxJSONDepth, "template variables")
	if err != nil {
		return SubmitEmailCommand{}, nil, err
	}
	metadata, err := normalizeMetadata(command.Metadata)
	if err != nil {
		return SubmitEmailCommand{}, nil, err
	}
	normalized := command
	normalized.RecipientEmail = email
	normalized.Locale = locale.String()
	normalized.Variables = variables
	normalized.DispatchDeadline = command.DispatchDeadline.UTC()
	if command.ScheduledAt != nil {
		value := command.ScheduledAt.UTC()
		normalized.ScheduledAt = &value
	}
	normalized.Metadata = nil
	return normalized, metadata, nil
}

func normalizeEmail(value string) (string, error) {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 254 || strings.ContainsAny(value, "\r\n") {
		return "", invalidSubmission("recipient email is invalid")
	}
	parsed, err := mail.ParseAddress(value)
	if err != nil || parsed.Name != "" || parsed.Address != value {
		return "", invalidSubmission("recipient email must be one bare address")
	}
	at := strings.LastIndexByte(parsed.Address, '@')
	if at <= 0 || at == len(parsed.Address)-1 {
		return "", invalidSubmission("recipient email is invalid")
	}
	domain, err := idna.Lookup.ToASCII(parsed.Address[at+1:])
	if err != nil {
		return "", invalidSubmission("recipient email domain is invalid")
	}
	normalized := parsed.Address[:at+1] + strings.ToLower(domain)
	if len(normalized) > 254 {
		return "", invalidSubmission("normalized recipient email is too long")
	}
	return normalized, nil
}

func validRegisteredKey(value string, maximum int) bool {
	return len(value) <= maximum && registeredKeyPattern.MatchString(value)
}

func normalizeMetadata(metadata map[string]string) (json.RawMessage, error) {
	if len(metadata) > maxMetadataEntries {
		return nil, invalidSubmission("metadata has too many entries")
	}
	if metadata == nil {
		return json.RawMessage(`{}`), nil
	}
	for key, value := range metadata {
		if !validRegisteredKey(key, 64) || len(value) > 256 || strings.ContainsAny(value, "\r\n") {
			return nil, invalidSubmission("metadata contains an invalid key or value")
		}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil || len(encoded) > 4*1024 {
		return nil, invalidSubmission("metadata exceeds 4 KiB")
	}
	return encoded, nil
}

func canonicalJSONObject(raw json.RawMessage, maximum, depth int, field string) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maximum || !json.Valid(raw) {
		return nil, invalidSubmission(field + " must be a bounded JSON object")
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, invalidSubmission(field + " is invalid")
	}
	if _, ok := value.(map[string]any); !ok || jsonDepth(value) > depth {
		return nil, invalidSubmission(field + " must be an object within the depth limit")
	}
	canonical, err := json.Marshal(value)
	if err != nil || len(canonical) > maximum {
		return nil, invalidSubmission(field + " cannot be canonicalized")
	}
	return canonical, nil
}

func jsonDepth(value any) int {
	switch typed := value.(type) {
	case map[string]any:
		maximum := 1
		for _, child := range typed {
			if depth := 1 + jsonDepth(child); depth > maximum {
				maximum = depth
			}
		}
		return maximum
	case []any:
		maximum := 1
		for _, child := range typed {
			if depth := 1 + jsonDepth(child); depth > maximum {
				maximum = depth
			}
		}
		return maximum
	default:
		return 0
	}
}

func validateResolvedTemplate(command SubmitEmailCommand, resolved ports.ResolvedTemplate) error {
	if resolved.Key != command.TemplateKey || resolved.Version == 0 || resolved.Locale == "" {
		return fmt.Errorf("%w: resolver returned mismatched template identity", ErrSubmissionInvariant)
	}
	if command.TemplateVersion != nil && resolved.Version != *command.TemplateVersion {
		return fmt.Errorf("%w: resolver changed requested template version", ErrSubmissionInvariant)
	}
	if resolved.Locale != command.Locale {
		return fmt.Errorf("%w: resolver changed requested locale", ErrSubmissionInvariant)
	}
	variables, err := canonicalJSONObject(resolved.Variables, maxVariablesBytes, maxJSONDepth, "resolved template variables")
	if err != nil {
		return fmt.Errorf("%w: resolver returned invalid variables", ErrSubmissionInvariant)
	}
	resolved.Variables = variables
	return nil
}

type canonicalPayload struct {
	RecipientEmail       string                    `json:"recipient_email"`
	RecipientDisplayName string                    `json:"recipient_display_name,omitempty"`
	SenderIdentityKey    string                    `json:"sender_identity_key"`
	TemplateKey          string                    `json:"template_key"`
	TemplateVersion      uint32                    `json:"template_version"`
	Locale               string                    `json:"locale"`
	Variables            json.RawMessage           `json:"variables"`
	Category             ports.EmailCategory       `json:"category"`
	Priority             uint8                     `json:"priority"`
	ScheduledAt          *time.Time                `json:"scheduled_at,omitempty"`
	DispatchDeadline     time.Time                 `json:"dispatch_deadline"`
	DuplicateRiskPolicy  ports.DuplicateRiskPolicy `json:"duplicate_risk_policy"`
	Metadata             json.RawMessage           `json:"metadata"`
}

func canonicalSubmissionPayload(
	command SubmitEmailCommand,
	resolved ports.ResolvedTemplate,
	metadata json.RawMessage,
) ([]byte, error) {
	variables, err := canonicalJSONObject(resolved.Variables, maxVariablesBytes, maxJSONDepth, "resolved template variables")
	if err != nil {
		return nil, fmt.Errorf("%w: resolver returned invalid variables", ErrSubmissionInvariant)
	}
	payload, err := json.Marshal(canonicalPayload{
		RecipientEmail:       command.RecipientEmail,
		RecipientDisplayName: command.RecipientDisplayName,
		SenderIdentityKey:    command.SenderIdentityKey,
		TemplateKey:          resolved.Key,
		TemplateVersion:      resolved.Version,
		Locale:               resolved.Locale,
		Variables:            variables,
		Category:             command.Category,
		Priority:             command.Priority,
		ScheduledAt:          command.ScheduledAt,
		DispatchDeadline:     command.DispatchDeadline,
		DuplicateRiskPolicy:  command.DuplicateRiskPolicy,
		Metadata:             metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: encode canonical payload", ErrSubmissionInvariant)
	}
	return payload, nil
}

func validateSubmissionTimes(now time.Time, scheduledAt *time.Time, deadline time.Time) error {
	now = now.UTC()
	deadline = deadline.UTC()
	if !deadline.After(now) {
		return invalidSubmission("dispatch deadline must be after acceptance")
	}
	if scheduledAt != nil {
		scheduled := scheduledAt.UTC()
		if !scheduled.Before(deadline) {
			return invalidSubmission("scheduled time must be before dispatch deadline")
		}
		if scheduled.After(now.Add(maxScheduledWindow)) {
			return invalidSubmission("scheduled time exceeds the 365 day window")
		}
	}
	return nil
}

func maxAttemptsFor(category ports.EmailCategory) uint32 {
	switch category {
	case ports.EmailCategoryCritical:
		return 3
	case ports.EmailCategoryTransactional:
		return 5
	case ports.EmailCategoryNotification:
		return 8
	case ports.EmailCategoryBulk:
		return 10
	default:
		return 1
	}
}

func maskEmail(email string) string {
	at := strings.LastIndexByte(email, '@')
	if at <= 0 {
		return "***"
	}
	local := []rune(email[:at])
	visible := string(local[0])
	if len(local) > 1 {
		visible += "***"
	}
	return visible + email[at:]
}

func cloneUint32(value *uint32) *uint32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func invalidSubmission(detail string) error {
	return fmt.Errorf("%w: %s", ErrInvalidSubmission, detail)
}
