package smtp

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestConfigValidation(t *testing.T) {
	t.Parallel()
	config := validTestConfig()
	if err := config.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if config.Address() != "smtp.example.com:465" {
		t.Fatalf("address = %q", config.Address())
	}

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "host injection", mutate: func(c *Config) { c.Host = "smtp.example.com\r\nX: y" }},
		{name: "plaintext mode", mutate: func(c *Config) { c.Security = "none" }},
		{name: "auth method", mutate: func(c *Config) { c.AuthMethod = "auto" }},
		{name: "empty auth code", mutate: func(c *Config) { c.AuthCode = "" }},
		{name: "credential newline", mutate: func(c *Config) { c.AuthCode = "secret\nvalue" }},
		{name: "from display address", mutate: func(c *Config) { c.FromAddress = "Sender <sender@example.com>" }},
		{name: "from header injection", mutate: func(c *Config) { c.FromName = "Sender\r\nBcc: x@example.com" }},
		{name: "timeout", mutate: func(c *Config) { c.SessionTimeout = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validTestConfig()
			test.mutate(&config)
			if err := config.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("validation error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestConfigErrorsDoNotLeakAuthorizationCode(t *testing.T) {
	t.Parallel()
	config := validTestConfig()
	secret := "never-print-this-auth-code"
	config.AuthCode = secret + "\n"
	err := config.Validate()
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe configuration error: %v", err)
	}
}

func validTestConfig() Config {
	return Config{
		Host:           "smtp.example.com",
		Port:           465,
		Security:       SecurityImplicitTLS,
		AuthMethod:     AuthLogin,
		Username:       "sender@example.com",
		AuthCode:       "authorization-code",
		FromAddress:    "sender@example.com",
		FromName:       "Mail Service",
		SessionTimeout: time.Second,
	}
}
