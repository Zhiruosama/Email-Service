package rabbitmq

import (
	"errors"
	"testing"
	"time"
)

func TestDefaultConfigIsValid(t *testing.T) {
	config := DefaultConfig("amqp://guest:guest@localhost:5672/", "relay-1")
	if err := config.Validate(); err != nil {
		t.Fatalf("validate default config: %v", err)
	}
	if config.Routes["MESSAGE_DISPATCH_REQUESTED"] != RoutingKeyDispatchRequested {
		t.Fatalf("dispatch route = %q", config.Routes["MESSAGE_DISPATCH_REQUESTED"])
	}
	if len(config.Queues) != 2 {
		t.Fatalf("default queue count = %d, want 2", len(config.Queues))
	}
}

func TestConfigValidateRejectsInvalidValues(t *testing.T) {
	valid := DefaultConfig("amqp://guest:guest@localhost:5672/", "relay-1")
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "empty URL", mutate: func(config *Config) { config.URL = "" }},
		{name: "URL scheme", mutate: func(config *Config) { config.URL = "http://localhost" }},
		{name: "URL host", mutate: func(config *Config) { config.URL = "amqp:///vhost" }},
		{name: "connection name", mutate: func(config *Config) { config.ConnectionName = "bad\nname" }},
		{name: "application id", mutate: func(config *Config) { config.ApplicationID = "" }},
		{name: "exchange", mutate: func(config *Config) { config.Exchange = " mail.events" }},
		{name: "pool zero", mutate: func(config *Config) { config.ChannelPoolSize = 0 }},
		{name: "pool too large", mutate: func(config *Config) { config.ChannelPoolSize = maxPublisherChannels + 1 }},
		{name: "connect timeout", mutate: func(config *Config) { config.ConnectTimeout = time.Millisecond }},
		{name: "heartbeat", mutate: func(config *Config) { config.Heartbeat = 0 }},
		{name: "close timeout", mutate: func(config *Config) { config.CloseTimeout = time.Minute }},
		{name: "no routes", mutate: func(config *Config) { config.Routes = nil }},
		{name: "no queues", mutate: func(config *Config) { config.Queues = nil }},
		{name: "invalid event", mutate: func(config *Config) {
			config.Routes["message accepted"] = RoutingKeyMessageAccepted
		}},
		{name: "invalid route", mutate: func(config *Config) {
			config.Routes["MESSAGE_ACCEPTED"] = "Mail.Accepted"
		}},
		{name: "unbound route", mutate: func(config *Config) {
			config.Routes["MESSAGE_ACCEPTED"] = "mail.unbound.v1"
		}},
		{name: "duplicate queue", mutate: func(config *Config) {
			config.Queues = append(config.Queues, config.Queues[0])
		}},
		{name: "empty queue name", mutate: func(config *Config) { config.Queues[0].Name = "" }},
		{name: "empty bindings", mutate: func(config *Config) { config.Queues[0].BindingKeys = nil }},
		{name: "invalid binding", mutate: func(config *Config) {
			config.Queues[0].BindingKeys[0] = "mail.#"
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := valid.clone()
			test.mutate(&config)
			if err := config.Validate(); !errors.Is(err, ErrInvalidRabbitMQConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidRabbitMQConfig", err)
			}
		})
	}
}

func TestConfigCloneIsDeep(t *testing.T) {
	original := DefaultConfig("amqp://guest:guest@localhost:5672/", "relay-1")
	cloned := original.clone()
	cloned.Routes["MESSAGE_ACCEPTED"] = "mail.changed.v1"
	cloned.Queues[0].BindingKeys[0] = "mail.changed.v1"

	if original.Routes["MESSAGE_ACCEPTED"] != RoutingKeyMessageAccepted {
		t.Fatal("route map was not cloned")
	}
	if original.Queues[0].BindingKeys[0] != RoutingKeyDispatchRequested {
		t.Fatal("queue binding slice was not cloned")
	}
}
