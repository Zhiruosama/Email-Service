package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

func TestNormalizeSubmissionCanonicalizesAddressLocaleAndMetadata(t *testing.T) {
	command := validSubmitCommand()
	command.RecipientEmail = "Alice@Example.COM"
	command.Locale = "zh-cn"
	command.Metadata = map[string]string{"trace": "abc", "source": "test"}
	command.DispatchDeadline = command.DispatchDeadline.Add(789 * time.Nanosecond)
	scheduledAt := command.DispatchDeadline.Add(-time.Minute).Add(456 * time.Nanosecond)
	command.ScheduledAt = &scheduledAt

	normalized, metadata, err := normalizeSubmission(command)
	if err != nil {
		t.Fatalf("normalize submission: %v", err)
	}
	if normalized.RecipientEmail != "Alice@example.com" {
		t.Fatalf("normalized email = %q", normalized.RecipientEmail)
	}
	if normalized.Locale != "zh-CN" {
		t.Fatalf("normalized locale = %q", normalized.Locale)
	}
	if string(metadata) != `{"source":"test","trace":"abc"}` {
		t.Fatalf("canonical metadata = %s", metadata)
	}
	if normalized.Metadata != nil {
		t.Fatal("normalized command retained the source metadata map")
	}
	if normalized.DispatchDeadline.Nanosecond()%1_000 != 0 ||
		normalized.ScheduledAt == nil || normalized.ScheduledAt.Nanosecond()%1_000 != 0 {
		t.Fatal("timestamps were not normalized to PostgreSQL microsecond precision")
	}
}

func TestNormalizeSubmissionTreatsOmittedMetadataAsEmptyObject(t *testing.T) {
	command := validSubmitCommand()
	command.Metadata = nil
	_, metadata, err := normalizeSubmission(command)
	if err != nil {
		t.Fatalf("normalize submission: %v", err)
	}
	if string(metadata) != `{}` {
		t.Fatalf("metadata = %s, want empty object", metadata)
	}
}

func TestNormalizeSubmissionRejectsUnsafeInputs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*SubmitEmailCommand)
	}{
		{name: "tenant", mutate: func(c *SubmitEmailCommand) { c.TenantID = "tenant" }},
		{name: "idempotency", mutate: func(c *SubmitEmailCommand) { c.IdempotencyKey = "bad key" }},
		{name: "display newline", mutate: func(c *SubmitEmailCommand) { c.RecipientDisplayName = "a\nb" }},
		{name: "address list", mutate: func(c *SubmitEmailCommand) { c.RecipientEmail = "a@example.com,b@example.com" }},
		{name: "sender", mutate: func(c *SubmitEmailCommand) { c.SenderIdentityKey = "bad key" }},
		{name: "locale", mutate: func(c *SubmitEmailCommand) { c.Locale = "not_a_locale" }},
		{name: "variables array", mutate: func(c *SubmitEmailCommand) { c.Variables = json.RawMessage(`[]`) }},
		{name: "priority", mutate: func(c *SubmitEmailCommand) { c.Priority = 10 }},
		{name: "deadline", mutate: func(c *SubmitEmailCommand) { c.DispatchDeadline = time.Time{} }},
		{name: "metadata newline", mutate: func(c *SubmitEmailCommand) { c.Metadata = map[string]string{"key": "secret\nvalue"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := validSubmitCommand()
			test.mutate(&command)
			if _, _, err := normalizeSubmission(command); !errors.Is(err, ErrInvalidSubmission) {
				t.Fatalf("normalize error = %v, want ErrInvalidSubmission", err)
			}
		})
	}
}

func TestValidateSubmissionTimesUsesAcceptanceClock(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	if err := validateSubmissionTimes(now, &past, now.Add(time.Minute)); err != nil {
		t.Fatalf("past schedule should be immediate: %v", err)
	}
	tooFar := now.Add(maxScheduledWindow + time.Second)
	if err := validateSubmissionTimes(now, &tooFar, tooFar.Add(time.Minute)); !errors.Is(err, ErrInvalidSubmission) {
		t.Fatalf("schedule-window error = %v, want ErrInvalidSubmission", err)
	}
	if err := validateSubmissionTimes(now, nil, now); !errors.Is(err, ErrInvalidSubmission) {
		t.Fatalf("deadline error = %v, want ErrInvalidSubmission", err)
	}
}

func TestNewEmailSubmissionServiceRejectsNilDependencies(t *testing.T) {
	tests := []func(){
		func() { NewEmailSubmissionService(nil, nilTemplateResolver{}, nilProtector{}) },
		func() { NewEmailSubmissionService(noCallTransactor{}, nil, nilProtector{}) },
		func() { NewEmailSubmissionService(noCallTransactor{}, nilTemplateResolver{}, nil) },
	}
	for index, construct := range tests {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("case %d did not panic", index)
				}
			}()
			construct()
		}()
	}
}

func validSubmitCommand() SubmitEmailCommand {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	return SubmitEmailCommand{
		TenantID:             "81000000-0000-4000-8000-000000000001",
		IdempotencyKey:       "request-1",
		RecipientEmail:       "alice@example.com",
		RecipientDisplayName: "Alice",
		SenderIdentityKey:    "default.sender",
		TemplateKey:          "verification_code.v1",
		Locale:               "zh-CN",
		Variables:            json.RawMessage(`{"code":"123456"}`),
		Category:             ports.EmailCategoryCritical,
		Priority:             9,
		DispatchDeadline:     now.Add(2 * time.Minute),
		DuplicateRiskPolicy:  ports.DuplicateRiskAvoidDuplicate,
		Metadata:             map[string]string{},
	}
}

type nilTemplateResolver struct{}

func (nilTemplateResolver) AuthorizeSender(context.Context, string, string) error {
	panic("must not be called")
}

func (nilTemplateResolver) Resolve(_ context.Context, _ ports.ResolveTemplateRequest) (ports.ResolvedTemplate, error) {
	panic("must not be called")
}

type nilProtector struct{}

func (nilProtector) Fingerprint([]byte) [32]byte { panic("must not be called") }
func (nilProtector) Seal(context.Context, string, []byte) (ports.ProtectedPayload, error) {
	panic("must not be called")
}
func (nilProtector) Open(context.Context, string, ports.ProtectedPayload) ([]byte, error) {
	panic("must not be called")
}
