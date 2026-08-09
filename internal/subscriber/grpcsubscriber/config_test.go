package grpcsubscriber

import (
	"errors"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	if err := (Config{Address: "dns:///ai-nexus:8443"}).Validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	for _, address := range []string{"", " ai-nexus:8443", "ai-nexus:8443\n"} {
		if err := (Config{Address: address}).Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("address %q error = %v, want ErrInvalidConfig", address, err)
		}
	}
}
