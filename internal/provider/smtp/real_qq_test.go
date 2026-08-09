//go:build real_smtp

package smtp

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/content/mimebuilder"
)

// TestRealQQSMTP is intentionally guarded by both a build tag and an explicit
// environment opt-in. It is never part of ordinary unit or integration tests.
func TestRealQQSMTP(t *testing.T) {
	if os.Getenv("MAIL_SMTP_REAL_TEST_ENABLED") != "true" {
		t.Skip("real SMTP test requires explicit MAIL_SMTP_REAL_TEST_ENABLED=true")
	}
	config := realSMTPConfig(t)
	recipient := requiredRealSMTPEnvironment(t, "MAIL_SMTP_TEST_RECIPIENT")
	if !validBareAddress(recipient) {
		t.Fatal("MAIL_SMTP_TEST_RECIPIENT must be one bare email address")
	}
	provider, err := New(config)
	if err != nil {
		t.Fatalf("create real SMTP provider: %v", err)
	}
	attemptID := "cd000000-0000-4000-8000-000000000001"
	request := ports.ProviderRequest{
		AttemptID:           attemptID,
		MessageID:           "cd000000-0000-4000-8000-000000000002",
		TenantID:            "cd000000-0000-4000-8000-000000000003",
		AttemptNumber:       1,
		DispatchGeneration:  1,
		Category:            ports.EmailCategoryCritical,
		DuplicateRiskPolicy: ports.DuplicateRiskAvoidDuplicate,
		Material: ports.DeliveryMaterial{
			EnvelopeFrom: config.FromAddress,
			EnvelopeTo:   recipient,
			MIMEMessage:  realSMTPMessage(t, config, recipient, attemptID),
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), config.SessionTimeout)
	defer cancel()
	result := provider.Submit(ctx, request)
	if result.Outcome != ports.ProviderOutcomeAccepted {
		t.Fatalf("real SMTP smoke test result = outcome:%s failure:%#v", result.Outcome, result.Failure)
	}
}

func realSMTPConfig(t *testing.T) Config {
	t.Helper()
	config := DefaultConfig()
	config.Host = requiredRealSMTPEnvironment(t, "MAIL_SMTP_HOST")
	port, err := strconv.ParseUint(requiredRealSMTPEnvironment(t, "MAIL_SMTP_PORT"), 10, 16)
	if err != nil || port == 0 {
		t.Fatal("MAIL_SMTP_PORT must be an integer in range 1..65535")
	}
	config.Port = uint16(port)
	config.Security = SecurityMode(requiredRealSMTPEnvironment(t, "MAIL_SMTP_SECURITY"))
	config.AuthMethod = AuthMethod(environmentOrTest("MAIL_SMTP_AUTH_METHOD", string(AuthLogin)))
	config.Username = requiredRealSMTPEnvironment(t, "MAIL_SMTP_USERNAME")
	config.AuthCode = requiredRealSMTPEnvironment(t, "MAIL_SMTP_AUTH_CODE")
	config.FromAddress = requiredRealSMTPEnvironment(t, "MAIL_SMTP_FROM_ADDRESS")
	config.FromName = environmentOrTest("MAIL_SMTP_FROM_NAME", "")
	if timeout := os.Getenv("MAIL_SMTP_SESSION_TIMEOUT"); timeout != "" {
		parsed, err := time.ParseDuration(timeout)
		if err != nil {
			t.Fatal("MAIL_SMTP_SESSION_TIMEOUT must be a Go duration")
		}
		config.SessionTimeout = parsed
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("real SMTP configuration rejected: %v", err)
	}
	return config
}

func realSMTPMessage(t *testing.T, config Config, recipient, attemptID string) []byte {
	t.Helper()
	encoded, err := mimebuilder.New().Encode(ports.MIMEMessageRequest{
		AttemptID: attemptID,
		Date:      time.Now().UTC(),
		Sender: ports.SenderIdentity{
			Key:         "smtp.smoke",
			Address:     config.FromAddress,
			DisplayName: config.FromName,
		},
		RecipientAddress: recipient,
		Content: ports.RenderedEmail{
			Subject:  "Mail Service SMTP 连通性测试",
			TextBody: "这是一封由 Mail Service 显式触发的 QQ SMTP 连通性测试邮件。",
			HTMLBody: "<p>这是一封由 Mail Service 显式触发的 QQ SMTP 连通性测试邮件。</p>",
		},
	})
	if err != nil {
		t.Fatalf("build real SMTP smoke message: %v", err)
	}
	return encoded
}

func requiredRealSMTPEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required for the real SMTP test", name)
	}
	return value
}

func environmentOrTest(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func TestRealSMTPMessageIsValidMaterial(t *testing.T) {
	config := DefaultConfig()
	config.FromAddress = "sender@example.com"
	config.FromName = "Mail Service"
	material := ports.DeliveryMaterial{
		EnvelopeFrom: config.FromAddress,
		EnvelopeTo:   "recipient@example.com",
		MIMEMessage: realSMTPMessage(
			t,
			config,
			"recipient@example.com",
			"cd000000-0000-4000-8000-000000000001",
		),
	}
	if err := material.Validate(); err != nil {
		t.Fatalf("real SMTP smoke message is invalid: %v", err)
	}
	if strings.Contains(string(material.MIMEMessage), "authorization-code") {
		t.Fatal("smoke message contains an authorization code")
	}
}
