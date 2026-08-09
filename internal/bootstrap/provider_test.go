package bootstrap

import (
	"errors"
	"strings"
	"testing"

	providersmtp "github.com/Zhiruosama/Email-Service/internal/provider/smtp"
)

func TestNewEmailProviderSelectsProviderAndSenderIdentity(t *testing.T) {
	t.Parallel()
	fakeConfig := Config{Provider: FakeProvider}
	fake, fakeSender, err := newEmailProvider(fakeConfig)
	if err != nil {
		t.Fatalf("new Fake provider: %v", err)
	}
	if fake.Key() != FakeProvider || fakeSender.Address != "no-reply@example.invalid" {
		t.Fatalf("fake provider/sender = %q/%#v", fake.Key(), fakeSender)
	}

	smtpConfig := Config{Provider: SMTPProvider, SMTP: providersmtp.DefaultConfig()}
	smtpConfig.SMTP.Username = "sender@qq.com"
	smtpConfig.SMTP.AuthCode = "test-authorization-code"
	smtpConfig.SMTP.FromAddress = "sender@qq.com"
	smtpConfig.SMTP.FromName = "AI Nexus"
	provider, sender, err := newEmailProvider(smtpConfig)
	if err != nil {
		t.Fatalf("new SMTP provider: %v", err)
	}
	if provider.Key() != SMTPProvider || sender.Address != "sender@qq.com" || sender.DisplayName != "AI Nexus" {
		t.Fatalf("SMTP provider/sender = %q/%#v", provider.Key(), sender)
	}
}

func TestNewEmailProviderRejectsSMTPConfigWithoutLeakingSecret(t *testing.T) {
	t.Parallel()
	secret := "do-not-print-smtp-secret"
	config := Config{Provider: SMTPProvider, SMTP: providersmtp.DefaultConfig()}
	config.SMTP.Username = "sender@qq.com"
	config.SMTP.AuthCode = secret
	config.SMTP.FromAddress = "invalid address"
	_, _, err := newEmailProvider(config)
	if !errors.Is(err, ErrStartup) || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe SMTP provider error: %v", err)
	}
}
