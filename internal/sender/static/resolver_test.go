package static

import (
	"context"
	"errors"
	"testing"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

func TestResolverReturnsOnlyRegisteredTenantIdentity(t *testing.T) {
	identity := ports.SenderIdentity{Key: "ainexus.default", Address: "no-reply@example.invalid", DisplayName: "AI Nexus"}
	resolver, err := New("aa000000-0000-4000-8000-000000000001", identity)
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	got, err := resolver.ResolveSender(context.Background(), "aa000000-0000-4000-8000-000000000001", identity.Key)
	if err != nil || got != identity {
		t.Fatalf("resolved identity = %#v/%v", got, err)
	}
	if _, err := resolver.ResolveSender(context.Background(), "aa000000-0000-4000-8000-000000000002", identity.Key); !errors.Is(err, ports.ErrSenderIdentityNotAllowed) {
		t.Fatalf("cross-tenant sender error = %v", err)
	}
}

func TestResolverRejectsInvalidRegistration(t *testing.T) {
	if _, err := New("tenant", ports.SenderIdentity{}); !errors.Is(err, ports.ErrInvalidSenderIdentity) {
		t.Fatalf("invalid registration error = %v", err)
	}
}
