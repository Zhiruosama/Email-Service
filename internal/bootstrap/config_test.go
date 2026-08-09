package bootstrap

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigDefaultsAndOverrides(t *testing.T) {
	t.Parallel()
	environment := map[string]string{
		"DATABASE_URL":                   "postgres://mail:secret@localhost:5432/mail?sslmode=disable",
		"RABBITMQ_URL":                   "amqp://mail:secret@localhost:5672/",
		"MAIL_PROVIDER":                  "fake",
		"MAIL_INSTANCE_ID":               "worker-a",
		"MAIL_GRPC_LISTEN_ADDRESS":       "127.0.0.1:9090",
		"MAIL_DATABASE_MAX_CONNS":        "32",
		"MAIL_SCHEDULER_BATCH_SIZE":      "64",
		"MAIL_RELAY_PUBLISH_CONCURRENCY": "4",
		"MAIL_CONSUMER_LANES":            "6",
		"MAIL_PROVIDER_TIMEOUT":          "12s",
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
	if config.Publisher.ConnectionName != "mail-publisher-worker-a" ||
		config.Consumer.ConnectionName != "mail-worker-worker-a" {
		t.Fatalf("RabbitMQ connection names = %q/%q", config.Publisher.ConnectionName, config.Consumer.ConnectionName)
	}
}

func TestLoadConfigRequiresSecretsWithoutLeakingValues(t *testing.T) {
	t.Parallel()
	secret := "postgres://user:do-not-print@localhost:5432/mail"
	tests := []map[string]string{
		{},
		{"DATABASE_URL": secret},
		{
			"DATABASE_URL": secret,
			"RABBITMQ_URL": "amqp://user:do-not-print@localhost:5672/",
		},
	}
	for _, environment := range tests {
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
	base := map[string]string{
		"DATABASE_URL":  "postgres://mail:secret@localhost:5432/mail",
		"RABBITMQ_URL":  "amqp://mail:secret@localhost:5672/",
		"MAIL_PROVIDER": "fake",
	}
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "duration syntax", key: "MAIL_PROVIDER_TIMEOUT", value: "soon"},
		{name: "integer syntax", key: "MAIL_CONSUMER_LANES", value: "many"},
		{name: "unsupported provider", key: "MAIL_PROVIDER", value: "smtp"},
		{name: "unsafe instance", key: "MAIL_INSTANCE_ID", value: "bad/id"},
		{name: "pool bounds", key: "MAIL_DATABASE_MIN_CONNS", value: "100"},
		{name: "consumer shutdown", key: "MAIL_CONSUMER_SHUTDOWN_TIMEOUT", value: "50s"},
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

func mapLookup(environment map[string]string) environmentLookup {
	return func(name string) (string, bool) {
		value, exists := environment[name]
		return value, exists
	}
}
