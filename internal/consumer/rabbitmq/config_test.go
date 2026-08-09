package rabbitmq

import (
	"errors"
	"testing"
)

func TestDefaultConfigAndValidation(t *testing.T) {
	t.Parallel()
	config := DefaultConfig("amqp://guest:guest@localhost:5672/", "instance-1")
	if err := config.Validate(); err != nil {
		t.Fatalf("default config rejected: %v", err)
	}
	if config.PrefetchPerLane != 1 || config.LaneCount != 4 {
		t.Fatalf("default lane config = %d/%d", config.LaneCount, config.PrefetchPerLane)
	}

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "URL", mutate: func(c *Config) { c.URL = "http://localhost" }},
		{name: "connection name", mutate: func(c *Config) { c.ConnectionName = " bad" }},
		{name: "same exchange", mutate: func(c *Config) { c.DeadLetterExchange = c.Exchange }},
		{name: "same queue", mutate: func(c *Config) { c.DeadLetterQueue = c.Queue }},
		{name: "zero lanes", mutate: func(c *Config) { c.LaneCount = 0 }},
		{name: "zero prefetch", mutate: func(c *Config) { c.PrefetchPerLane = 0 }},
		{name: "connect timeout", mutate: func(c *Config) { c.ConnectTimeout = 0 }},
		{name: "reconnect backoff", mutate: func(c *Config) { c.ReconnectCap = 0 }},
		{name: "requeue backoff", mutate: func(c *Config) { c.TransientRequeueBase = 0 }},
		{name: "shutdown timeout", mutate: func(c *Config) { c.ShutdownTimeout = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := config
			test.mutate(&candidate)
			if err := candidate.Validate(); !errors.Is(err, ErrInvalidConsumerConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidConsumerConfig", err)
			}
		})
	}
}

func TestDefaultLifecycleConfigUsesIsolatedTopologyAndBindings(t *testing.T) {
	t.Parallel()
	config := DefaultLifecycleConfig("amqp://guest:guest@localhost:5672/", "instance-1")
	if err := config.Validate(); err != nil {
		t.Fatalf("lifecycle config rejected: %v", err)
	}
	if err := config.ValidateLifecycle(); err != nil {
		t.Fatalf("lifecycle route contract rejected: %v", err)
	}
	if config.Queue != "mail.lifecycle.v1.q" ||
		config.DeadLetterQueue != "mail.lifecycle.dead.v1.q" ||
		config.ConnectionName != "mail-notifier-instance-1" ||
		len(config.AdditionalRoutingKeys) != 1 {
		t.Fatalf("unexpected lifecycle config: %#v", config)
	}

	duplicate := config
	duplicate.AdditionalRoutingKeys = []string{config.RoutingKey}
	if err := duplicate.Validate(); !errors.Is(err, ErrInvalidConsumerConfig) {
		t.Fatalf("duplicate routing key error = %v, want ErrInvalidConsumerConfig", err)
	}
	missing := config
	missing.AdditionalRoutingKeys = nil
	if err := missing.ValidateLifecycle(); !errors.Is(err, ErrInvalidConsumerConfig) {
		t.Fatalf("missing lifecycle route error = %v, want ErrInvalidConsumerConfig", err)
	}
}
