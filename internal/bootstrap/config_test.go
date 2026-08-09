package bootstrap

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const (
	testEncryptionKeyBase64  = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
	testFingerprintKeyBase64 = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk="
)

func TestLoadConfigDefaultsAndOverrides(t *testing.T) {
	t.Parallel()
	environment := map[string]string{
		"DATABASE_URL":                            "postgres://mail:secret@localhost:5432/mail?sslmode=disable",
		"RABBITMQ_URL":                            "amqp://mail:secret@localhost:5672/",
		"MAIL_PROVIDER":                           "fake",
		"MAIL_INSTANCE_ID":                        "worker-a",
		"MAIL_GRPC_LISTEN_ADDRESS":                "127.0.0.1:9090",
		"MAIL_DATABASE_MAX_CONNS":                 "32",
		"MAIL_SCHEDULER_BATCH_SIZE":               "64",
		"MAIL_RELAY_PUBLISH_CONCURRENCY":          "4",
		"MAIL_CONSUMER_LANES":                     "6",
		"MAIL_LIFECYCLE_CONSUMER_LANES":           "3",
		"MAIL_PROVIDER_TIMEOUT":                   "12s",
		"MAIL_PROVIDER_MAX_CONCURRENT":            "3",
		"MAIL_PROVIDER_RATE_PER_SECOND":           "2.5",
		"MAIL_PROVIDER_RATE_BURST":                "4",
		"MAIL_PROVIDER_CIRCUIT_FAILURE_THRESHOLD": "7",
		"MAIL_PROVIDER_CIRCUIT_OPEN_DURATION":     "45s",
		"MAIL_CALLBACK_TIMEOUT":                   "4s",
		"MAIL_GRPC_ALLOW_INSECURE":                "true",
		"MAIL_DEV_TENANT_ID":                      "10000000-0000-4000-8000-000000000001",
		"MAIL_CALLBACK_GRPC_ADDRESS":              "127.0.0.1:9091",
		"MAIL_CALLBACK_GRPC_ALLOW_INSECURE":       "true",
		"MAIL_PAYLOAD_KEY_ID":                     "dev-key-1",
		"MAIL_PAYLOAD_ENCRYPTION_KEY_BASE64":      testEncryptionKeyBase64,
		"MAIL_PAYLOAD_FINGERPRINT_KEY_BASE64":     testFingerprintKeyBase64,
	}
	config, err := loadConfig(mapLookup(environment), "ignored-host")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.InstanceID != "worker-a" || config.Provider != FakeProvider {
		t.Fatalf("identity/provider = %q/%q", config.InstanceID, config.Provider)
	}
	if config.Database.MaxConnections != 32 || config.SchedulerBatchSize != 64 {
		t.Fatalf("database/scheduler overrides = %d/%d", config.Database.MaxConnections, config.SchedulerBatchSize)
	}
	if config.Relay.PublishConcurrency != 4 || config.Consumer.LaneCount != 6 {
		t.Fatalf("relay/consumer overrides = %d/%d", config.Relay.PublishConcurrency, config.Consumer.LaneCount)
	}
	if config.Worker.ProviderTimeout != 12*time.Second {
		t.Fatalf("provider timeout = %s", config.Worker.ProviderTimeout)
	}
	if config.ProviderResilience.MaxConcurrent != 3 ||
		config.ProviderResilience.RatePerSecond != 2.5 ||
		config.ProviderResilience.Burst != 4 ||
		config.ProviderResilience.Circuit.FailureThreshold != 7 ||
		config.ProviderResilience.Circuit.OpenDuration != 45*time.Second {
		t.Fatalf("provider resilience overrides = %#v", config.ProviderResilience)
	}
	if config.NotificationWorker.CallbackTimeout != 4*time.Second ||
		config.LifecycleConsumer.LaneCount != 3 ||
		config.Callback.GRPC.Address != "127.0.0.1:9091" {
		t.Fatalf("notification overrides = %#v/%#v/%#v", config.NotificationWorker, config.LifecycleConsumer, config.Callback)
	}
	if config.Publisher.ConnectionName != "mail-publisher-worker-a" ||
		config.Consumer.ConnectionName != "mail-worker-worker-a" {
		t.Fatalf("RabbitMQ connection names = %q/%q", config.Publisher.ConnectionName, config.Consumer.ConnectionName)
	}
}

func TestLoadConfigRequiresSecretsWithoutLeakingValues(t *testing.T) {
	t.Parallel()
	secret := "postgres://user:do-not-print@localhost:5432/mail"
	base := validConfigEnvironment()
	base["DATABASE_URL"] = secret
	tests := []string{
		"DATABASE_URL",
		"RABBITMQ_URL",
		"MAIL_PROVIDER",
		"MAIL_GRPC_ALLOW_INSECURE",
		"MAIL_DEV_TENANT_ID",
		"MAIL_PAYLOAD_KEY_ID",
		"MAIL_PAYLOAD_ENCRYPTION_KEY_BASE64",
		"MAIL_PAYLOAD_FINGERPRINT_KEY_BASE64",
		"MAIL_CALLBACK_GRPC_ADDRESS",
		"MAIL_CALLBACK_GRPC_ALLOW_INSECURE",
	}
	for _, missing := range tests {
		environment := cloneEnvironment(base)
		delete(environment, missing)
		_, err := loadConfig(mapLookup(environment), "test-host")
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("load error = %v, want ErrInvalidConfig", err)
		}
		if strings.Contains(err.Error(), "do-not-print") {
			t.Fatalf("configuration error leaked a credential: %v", err)
		}
	}
}

func TestConfigRejectsInvalidOverrides(t *testing.T) {
	t.Parallel()
	base := validConfigEnvironment()
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "duration syntax", key: "MAIL_PROVIDER_TIMEOUT", value: "soon"},
		{name: "integer syntax", key: "MAIL_CONSUMER_LANES", value: "many"},
		{name: "provider concurrency", key: "MAIL_PROVIDER_MAX_CONCURRENT", value: "0"},
		{name: "provider rate syntax", key: "MAIL_PROVIDER_RATE_PER_SECOND", value: "fast"},
		{name: "provider rate finite", key: "MAIL_PROVIDER_RATE_PER_SECOND", value: "NaN"},
		{name: "provider burst", key: "MAIL_PROVIDER_RATE_BURST", value: "0"},
		{name: "circuit threshold", key: "MAIL_PROVIDER_CIRCUIT_FAILURE_THRESHOLD", value: "0"},
		{name: "circuit duration syntax", key: "MAIL_PROVIDER_CIRCUIT_OPEN_DURATION", value: "later"},
		{name: "circuit duration range", key: "MAIL_PROVIDER_CIRCUIT_OPEN_DURATION", value: "10ms"},
		{name: "unsupported provider", key: "MAIL_PROVIDER", value: "unsupported"},
		{name: "unsafe instance", key: "MAIL_INSTANCE_ID", value: "bad/id"},
		{name: "pool bounds", key: "MAIL_DATABASE_MIN_CONNS", value: "100"},
		{name: "consumer shutdown", key: "MAIL_CONSUMER_SHUTDOWN_TIMEOUT", value: "50s"},
		{name: "insecure acknowledgement", key: "MAIL_GRPC_ALLOW_INSECURE", value: "false"},
		{name: "tenant", key: "MAIL_DEV_TENANT_ID", value: "tenant"},
		{name: "key encoding", key: "MAIL_PAYLOAD_ENCRYPTION_KEY_BASE64", value: "not-secret-key-material"},
		{name: "same key purpose", key: "MAIL_PAYLOAD_FINGERPRINT_KEY_BASE64", value: testEncryptionKeyBase64},
		{name: "callback address", key: "MAIL_CALLBACK_GRPC_ADDRESS", value: " bad-address"},
		{name: "callback insecure acknowledgement", key: "MAIL_CALLBACK_GRPC_ALLOW_INSECURE", value: "false"},
		{name: "callback timeout", key: "MAIL_CALLBACK_TIMEOUT", value: "0s"},
		{name: "lifecycle consumer shutdown", key: "MAIL_LIFECYCLE_CONSUMER_SHUTDOWN_TIMEOUT", value: "50s"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			environment := make(map[string]string, len(base)+1)
			for key, value := range base {
				environment[key] = value
			}
			environment[test.key] = test.value
			if _, err := loadConfig(mapLookup(environment), "test-host"); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("load error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestLoadSMTPConfigOnlyWhenSelected(t *testing.T) {
	t.Parallel()
	environment := validConfigEnvironment()
	environment["MAIL_PROVIDER"] = SMTPProvider
	environment["MAIL_SMTP_HOST"] = "smtp.qq.com"
	environment["MAIL_SMTP_PORT"] = "465"
	environment["MAIL_SMTP_SECURITY"] = "implicit_tls"
	environment["MAIL_SMTP_USERNAME"] = "sender@qq.com"
	environment["MAIL_SMTP_AUTH_CODE"] = "test-authorization-code"
	environment["MAIL_SMTP_FROM_ADDRESS"] = "sender@qq.com"
	environment["MAIL_SMTP_FROM_NAME"] = "AI Nexus"

	config, err := loadConfig(mapLookup(environment), "test-host")
	if err != nil {
		t.Fatalf("load SMTP config: %v", err)
	}
	if config.Provider != SMTPProvider || config.SMTP.AuthMethod != "login" ||
		config.SMTP.Address() != "smtp.qq.com:465" || config.SMTP.FromName != "AI Nexus" {
		t.Fatalf(
			"unexpected SMTP safe config: provider=%q address=%q auth=%q from_name=%q",
			config.Provider,
			config.SMTP.Address(),
			config.SMTP.AuthMethod,
			config.SMTP.FromName,
		)
	}

	for _, missing := range []string{
		"MAIL_SMTP_HOST",
		"MAIL_SMTP_PORT",
		"MAIL_SMTP_SECURITY",
		"MAIL_SMTP_USERNAME",
		"MAIL_SMTP_AUTH_CODE",
		"MAIL_SMTP_FROM_ADDRESS",
	} {
		missingEnvironment := cloneEnvironment(environment)
		delete(missingEnvironment, missing)
		_, err := loadConfig(mapLookup(missingEnvironment), "test-host")
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("missing %s error = %v, want ErrInvalidConfig", missing, err)
		}
	}
}

func TestLoadSMTPConfigDoesNotLeakAuthorizationCode(t *testing.T) {
	t.Parallel()
	environment := validConfigEnvironment()
	environment["MAIL_PROVIDER"] = SMTPProvider
	environment["MAIL_SMTP_HOST"] = "smtp.qq.com"
	environment["MAIL_SMTP_PORT"] = "465"
	environment["MAIL_SMTP_SECURITY"] = "implicit_tls"
	environment["MAIL_SMTP_USERNAME"] = "sender@qq.com"
	secret := "do-not-print-this-smtp-code"
	environment["MAIL_SMTP_AUTH_CODE"] = secret + "\n"
	environment["MAIL_SMTP_FROM_ADDRESS"] = "sender@qq.com"
	_, err := loadConfig(mapLookup(environment), "test-host")
	if !errors.Is(err, ErrInvalidConfig) || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe SMTP configuration error: %v", err)
	}
}

func TestDefaultConfigFailsClosedWithoutSubmissionSecrets(t *testing.T) {
	config := DefaultConfig(
		"postgres://mail:secret@localhost:5432/mail",
		"amqp://mail:secret@localhost:5672/",
		"test-instance",
		FakeProvider,
	)
	if err := config.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("default config error = %v, want ErrInvalidConfig", err)
	}
}

func validConfigEnvironment() map[string]string {
	return map[string]string{
		"DATABASE_URL":                        "postgres://mail:secret@localhost:5432/mail",
		"RABBITMQ_URL":                        "amqp://mail:secret@localhost:5672/",
		"MAIL_PROVIDER":                       "fake",
		"MAIL_GRPC_ALLOW_INSECURE":            "true",
		"MAIL_DEV_TENANT_ID":                  "10000000-0000-4000-8000-000000000001",
		"MAIL_PAYLOAD_KEY_ID":                 "dev-key-1",
		"MAIL_PAYLOAD_ENCRYPTION_KEY_BASE64":  testEncryptionKeyBase64,
		"MAIL_PAYLOAD_FINGERPRINT_KEY_BASE64": testFingerprintKeyBase64,
		"MAIL_CALLBACK_GRPC_ADDRESS":          "127.0.0.1:9091",
		"MAIL_CALLBACK_GRPC_ALLOW_INSECURE":   "true",
	}
}

func cloneEnvironment(source map[string]string) map[string]string {
	copy := make(map[string]string, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func mapLookup(environment map[string]string) environmentLookup {
	return func(name string) (string, bool) {
		value, exists := environment[name]
		return value, exists
	}
}
