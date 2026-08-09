//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/db/migrations"
	deliveryapp "github.com/Zhiruosama/Email-Service/internal/application/delivery"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	payloadsecurity "github.com/Zhiruosama/Email-Service/internal/security/payload"
	postgresstore "github.com/Zhiruosama/Email-Service/internal/storage/postgres"
	"github.com/Zhiruosama/Email-Service/internal/testkit/postgrescontainer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
)

const submissionTenantID = "a1000000-0000-4000-8000-000000000001"

func TestEmailSubmissionIsAtomicIdempotentAndEncrypted(t *testing.T) {
	instance := postgrescontainer.StartInstance(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	goose.SetBaseFS(migrations.Files)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("set Goose dialect: %v", err)
	}
	if err := goose.UpContext(ctx, instance.SQL, "sql"); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	pool, err := pgxpool.New(ctx, instance.ConnectionString)
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `
		INSERT INTO tenants (id, key, name, default_locale)
		VALUES ($1, 'submission-test', 'Submission Test', 'zh-CN')
	`, submissionTenantID); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}

	protector, err := payloadsecurity.New(
		"integration-key-1",
		bytes.Repeat([]byte{0x31}, payloadsecurity.KeySize),
		bytes.Repeat([]byte{0x42}, payloadsecurity.KeySize),
	)
	if err != nil {
		t.Fatalf("create payload protector: %v", err)
	}
	service := deliveryapp.NewEmailSubmissionService(
		postgresstore.NewTransactionManager(pool),
		verificationTemplateResolver{},
		protector,
	)
	command := deliveryapp.SubmitEmailCommand{
		TenantID:            submissionTenantID,
		IdempotencyKey:      "ainexus-request-001",
		RecipientEmail:      "user@Example.COM",
		SenderIdentityKey:   "ainexus.default",
		TemplateKey:         "verification_code.v1",
		Locale:              "zh-CN",
		Variables:           json.RawMessage(`{"purpose":"LOGIN","code":"123456","valid_for_seconds":300}`),
		Category:            ports.EmailCategoryCritical,
		Priority:            9,
		DispatchDeadline:    time.Now().UTC().Add(2 * time.Minute),
		DuplicateRiskPolicy: ports.DuplicateRiskAvoidDuplicate,
		Metadata:            map[string]string{"request_source": "ai-nexus"},
	}

	accepted, err := service.Submit(ctx, command)
	if err != nil {
		t.Fatalf("submit email: %v", err)
	}
	if accepted.Disposition != deliveryapp.SubmitEmailAccepted {
		t.Fatalf("disposition = %q, want ACCEPTED", accepted.Disposition)
	}
	if accepted.Record.Submission == nil ||
		accepted.Record.Submission.TemplateVersion != 7 ||
		accepted.Record.Submission.RecipientMasked != "u***@example.com" {
		t.Fatalf("unexpected safe submission view: %#v", accepted.Record.Submission)
	}
	messageID := accepted.Record.Message.ID()

	duplicate, err := service.Submit(ctx, command)
	if err != nil {
		t.Fatalf("repeat same email: %v", err)
	}
	if duplicate.Disposition != deliveryapp.SubmitEmailDuplicate || duplicate.Record.Message.ID() != messageID {
		t.Fatalf("duplicate result = %#v, want original message %s", duplicate, messageID)
	}

	conflict := command
	conflict.Variables = json.RawMessage(`{"code":"654321","purpose":"LOGIN","valid_for_seconds":300}`)
	if _, err := service.Submit(ctx, conflict); !errors.Is(err, ports.ErrIdempotencyConflict) {
		t.Fatalf("conflicting payload error = %v, want ErrIdempotencyConflict", err)
	}

	var (
		encryptedPayload   []byte
		metadataText       string
		messageCount       int
		outboxCount        int
		deliveryEventCount int
	)
	if err := pool.QueryRow(ctx, `
		SELECT encrypted_payload, submission_metadata::text,
		       (SELECT count(*) FROM mail_messages WHERE tenant_id = $1),
		       (SELECT count(*) FROM outbox_events WHERE aggregate_id = $2),
		       (SELECT count(*) FROM delivery_events WHERE message_id = $2)
		FROM mail_messages
		WHERE id = $2
	`, submissionTenantID, messageID).Scan(
		&encryptedPayload,
		&metadataText,
		&messageCount,
		&outboxCount,
		&deliveryEventCount,
	); err != nil {
		t.Fatalf("inspect persisted submission: %v", err)
	}
	if messageCount != 1 || outboxCount != 3 || deliveryEventCount != 2 {
		t.Fatalf("message/outbox/event counts = %d/%d/%d, want 1/3/2", messageCount, outboxCount, deliveryEventCount)
	}
	if bytes.Contains(encryptedPayload, []byte("123456")) || metadataText != `{"request_source": "ai-nexus"}` {
		t.Fatalf("unexpected stored payload or metadata: ciphertext=%x metadata=%s", encryptedPayload, metadataText)
	}
	var secretInOutbox bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM outbox_events
			WHERE aggregate_id = $1 AND payload::text LIKE '%123456%'
		)
	`, messageID).Scan(&secretInOutbox); err != nil {
		t.Fatalf("inspect outbox secrecy: %v", err)
	}
	if secretInOutbox {
		t.Fatal("verification code leaked into outbox")
	}
	var matchedIdentities int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM delivery_events d
		JOIN outbox_events o ON o.id = d.id
		WHERE d.message_id = $1
	`, messageID).Scan(&matchedIdentities); err != nil {
		t.Fatalf("compare journal/outbox identities: %v", err)
	}
	if matchedIdentities != 2 {
		t.Fatalf("matching journal/outbox identities = %d, want 2", matchedIdentities)
	}
	var acceptedEventID string
	var acceptedOccurredAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT id, occurred_at
		FROM delivery_events
		WHERE message_id = $1 AND sequence = 1
	`, messageID).Scan(&acceptedEventID, &acceptedOccurredAt); err != nil {
		t.Fatalf("read accepted journal event: %v", err)
	}
	journalRepository := postgresstore.NewDeliveryEventRepository(pool)
	persistedAccepted, err := journalRepository.GetByID(ctx, acceptedEventID)
	if err != nil {
		t.Fatalf("get accepted journal event by id: %v", err)
	}
	if persistedAccepted.ID != acceptedEventID ||
		persistedAccepted.MessageID != messageID ||
		persistedAccepted.IdempotencyKey != command.IdempotencyKey ||
		persistedAccepted.Status != message.StatusAccepted ||
		persistedAccepted.Sequence != 1 ||
		persistedAccepted.ObservedAt.IsZero() {
		t.Fatalf("unexpected persisted accepted event: %#v", persistedAccepted)
	}
	if _, err := journalRepository.GetByID(
		ctx,
		"a1000000-0000-4000-8000-000000000099",
	); !errors.Is(err, ports.ErrDeliveryEventNotFound) {
		t.Fatalf("missing journal event error = %v, want ErrDeliveryEventNotFound", err)
	}
	acceptedEvent := ports.DeliveryEvent{
		ID:             acceptedEventID,
		TenantID:       submissionTenantID,
		MessageID:      messageID,
		IdempotencyKey: command.IdempotencyKey,
		Status:         message.StatusAccepted,
		Sequence:       1,
		OccurredAt:     acceptedOccurredAt,
	}
	if err := journalRepository.Append(ctx, []ports.DeliveryEvent{acceptedEvent}); err != nil {
		t.Fatalf("idempotently append same journal event: %v", err)
	}
	conflictingEvent := acceptedEvent
	conflictingEvent.Status = message.StatusQueued
	if err := journalRepository.Append(ctx, []ports.DeliveryEvent{conflictingEvent}); !errors.Is(err, ports.ErrDeliveryEventConflict) {
		t.Fatalf("conflicting journal event error = %v, want ErrDeliveryEventConflict", err)
	}
}

type verificationTemplateResolver struct{}

func (verificationTemplateResolver) AuthorizeSender(_ context.Context, tenantID, senderKey string) error {
	if tenantID != submissionTenantID || senderKey != "ainexus.default" {
		return ports.ErrSenderIdentityNotAllowed
	}
	return nil
}

func (verificationTemplateResolver) Resolve(
	_ context.Context,
	request ports.ResolveTemplateRequest,
) (ports.ResolvedTemplate, error) {
	if request.TemplateKey != "verification_code.v1" || request.Locale != "zh-CN" {
		return ports.ResolvedTemplate{}, ports.ErrTemplateNotFound
	}
	if request.RequestedVersion != nil && *request.RequestedVersion != 7 {
		return ports.ResolvedTemplate{}, ports.ErrTemplateNotFound
	}
	var variables struct {
		Code            string `json:"code"`
		Purpose         string `json:"purpose"`
		ValidForSeconds uint32 `json:"valid_for_seconds"`
	}
	if err := json.Unmarshal(request.Variables, &variables); err != nil ||
		len(variables.Code) != 6 || variables.Purpose != "LOGIN" ||
		variables.ValidForSeconds < 60 || variables.ValidForSeconds > 1800 {
		return ports.ResolvedTemplate{}, ports.ErrTemplateVariables
	}
	return ports.ResolvedTemplate{
		Key:       request.TemplateKey,
		Version:   7,
		Locale:    request.Locale,
		Variables: append(json.RawMessage(nil), request.Variables...),
	}, nil
}
