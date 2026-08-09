package delivery

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/content/mimebuilder"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	payloadsecurity "github.com/Zhiruosama/Email-Service/internal/security/payload"
	senderstatic "github.com/Zhiruosama/Email-Service/internal/sender/static"
	templatecatalog "github.com/Zhiruosama/Email-Service/internal/template/catalog"
)

func TestDeliveryMaterialServiceDecryptsRendersAndBuildsMIME(t *testing.T) {
	record, attempt, service := materialTestFixture(t)
	material, err := service.Build(context.Background(), record, attempt)
	if err != nil {
		t.Fatalf("build delivery material: %v", err)
	}
	if material.EnvelopeTo != "user@example.com" || material.EnvelopeFrom != "no-reply@example.invalid" {
		t.Fatalf("unexpected envelope: %#v", material)
	}
	encoded := string(material.MIMEMessage)
	if !strings.Contains(encoded, "123456") {
		t.Fatalf("MIME did not contain expected rendered content")
	}
	if strings.Contains(encoded, record.Submission.RecipientMasked) {
		t.Fatal("MIME used the persisted masked address instead of the decrypted recipient")
	}
}

func TestDeliveryMaterialServiceClassifiesUnavailableKeyAndTampering(t *testing.T) {
	record, attempt, service := materialTestFixture(t)
	record.Submission.PayloadKeyID = "retired-key"
	_, err := service.Build(context.Background(), record, attempt)
	assertMaterialError(t, err, "PAYLOAD_KEY_UNAVAILABLE", true)

	record, attempt, service = materialTestFixture(t)
	record.Submission.EncryptedPayload[len(record.Submission.EncryptedPayload)-1] ^= 0xff
	_, err = service.Build(context.Background(), record, attempt)
	assertMaterialError(t, err, "PAYLOAD_AUTHENTICATION_FAILED", false)
}

func TestDeliveryMaterialServiceRejectsPersistedIdentityMismatch(t *testing.T) {
	record, attempt, service := materialTestFixture(t)
	record.Submission.TemplateVersion = 2
	_, err := service.Build(context.Background(), record, attempt)
	assertMaterialError(t, err, "PAYLOAD_IDENTITY_MISMATCH", false)
}

func TestNewDeliveryMaterialServiceRejectsNilDependencies(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("constructor did not panic")
		}
	}()
	NewDeliveryMaterialService(nil, nil, nil, nil)
}

func materialTestFixture(t *testing.T) (ports.MessageRecord, ports.StartedDeliveryAttempt, *DeliveryMaterialService) {
	t.Helper()
	tenantID := "bc000000-0000-4000-8000-000000000001"
	messageID := "bc000000-0000-4000-8000-000000000002"
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	aggregate, err := message.New(message.NewParams{
		ID:               messageID,
		Now:              now,
		DispatchDeadline: now.Add(10 * time.Minute),
		MaxAttempts:      3,
	})
	if err != nil {
		t.Fatalf("new message: %v", err)
	}
	aggregate.PullEvents()
	if err := aggregate.StartSending(1, now.Add(time.Second)); err != nil {
		t.Fatalf("start sending: %v", err)
	}
	aggregate.PullEvents()
	protector, err := payloadsecurity.New(
		"material-key",
		bytes.Repeat([]byte{0x31}, payloadsecurity.KeySize),
		bytes.Repeat([]byte{0x42}, payloadsecurity.KeySize),
	)
	if err != nil {
		t.Fatalf("new protector: %v", err)
	}
	command := SubmitEmailCommand{
		TenantID:            tenantID,
		RecipientEmail:      "user@example.com",
		SenderIdentityKey:   templatecatalog.AINexusSenderIdentityKey,
		TemplateKey:         templatecatalog.VerificationCodeTemplateKey,
		Locale:              "zh-CN",
		Category:            ports.EmailCategoryCritical,
		Priority:            9,
		DispatchDeadline:    aggregate.DispatchDeadline(),
		DuplicateRiskPolicy: ports.DuplicateRiskAvoidDuplicate,
	}
	resolved := ports.ResolvedTemplate{
		Key:       command.TemplateKey,
		Version:   1,
		Locale:    command.Locale,
		Variables: []byte(`{"code":"123456","purpose":"LOGIN","valid_for_seconds":300}`),
	}
	plaintext, err := canonicalSubmissionPayload(command, resolved, []byte(`{}`))
	if err != nil {
		t.Fatalf("canonical payload: %v", err)
	}
	protected, err := protector.Seal(context.Background(), tenantID+"/"+messageID, plaintext)
	clear(plaintext)
	if err != nil {
		t.Fatalf("seal payload: %v", err)
	}
	record := ports.MessageRecord{
		TenantID:            tenantID,
		IdempotencyKey:      "material-test",
		PayloadFingerprint:  [32]byte{1},
		Category:            command.Category,
		Priority:            command.Priority,
		DuplicateRiskPolicy: command.DuplicateRiskPolicy,
		Submission: &ports.SubmissionDetails{
			SenderIdentityKey: command.SenderIdentityKey,
			TemplateKey:       resolved.Key,
			TemplateVersion:   resolved.Version,
			Locale:            resolved.Locale,
			RecipientMasked:   "u***@example.com",
			PayloadKeyID:      protected.KeyID,
			EncryptedPayload:  protected.Ciphertext,
			Metadata:          []byte(`{}`),
		},
		Message: aggregate,
	}
	attempt := ports.StartedDeliveryAttempt{
		ID:                 "bc000000-0000-4000-8000-000000000003",
		MessageID:          messageID,
		AttemptNumber:      1,
		DispatchGeneration: 1,
		ProviderKey:        "fake",
		StartedAt:          now.Add(time.Second),
	}
	renderer := templatecatalog.NewVerificationCatalog(tenantID)
	senders, err := senderstatic.New(tenantID, ports.SenderIdentity{
		Key:         templatecatalog.AINexusSenderIdentityKey,
		Address:     "no-reply@example.invalid",
		DisplayName: "AI Nexus",
	})
	if err != nil {
		t.Fatalf("new sender resolver: %v", err)
	}
	return record, attempt, NewDeliveryMaterialService(protector, renderer, senders, mimebuilder.New())
}

func assertMaterialError(t *testing.T, err error, code string, retryable bool) {
	t.Helper()
	var materialErr *ports.DeliveryMaterialError
	if !errors.As(err, &materialErr) || materialErr.Code != code || materialErr.Retryable != retryable {
		t.Fatalf("material error = %#v, want %s retryable=%t", err, code, retryable)
	}
}
