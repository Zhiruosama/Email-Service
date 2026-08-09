package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

func TestVerificationCatalogPinsVersionAndCanonicalizesVariables(t *testing.T) {
	catalog := NewVerificationCatalog("tenant-1")
	resolved, err := catalog.Resolve(context.Background(), ports.ResolveTemplateRequest{
		TenantID:    "tenant-1",
		TemplateKey: VerificationCodeTemplateKey,
		Locale:      "zh-CN",
		Variables:   json.RawMessage(`{"valid_for_seconds":300,"purpose":"LOGIN","code":"123456"}`),
	})
	if err != nil {
		t.Fatalf("resolve template: %v", err)
	}
	if resolved.Version != 1 || string(resolved.Variables) != `{"code":"123456","purpose":"LOGIN","valid_for_seconds":300}` {
		t.Fatalf("unexpected resolved template: %#v", resolved)
	}
}

func TestVerificationCatalogRejectsUnauthorizedOrInvalidRequests(t *testing.T) {
	catalog := NewVerificationCatalog("tenant-1")
	base := ports.ResolveTemplateRequest{
		TenantID:    "tenant-1",
		TemplateKey: VerificationCodeTemplateKey,
		Locale:      "zh-CN",
		Variables:   json.RawMessage(`{"code":"123456","purpose":"LOGIN","valid_for_seconds":300}`),
	}
	tests := []struct {
		name   string
		mutate func(*ports.ResolveTemplateRequest)
		want   error
	}{
		{name: "tenant", mutate: func(r *ports.ResolveTemplateRequest) { r.TenantID = "tenant-2" }, want: ports.ErrTemplateNotAllowed},
		{name: "locale", mutate: func(r *ports.ResolveTemplateRequest) { r.Locale = "en" }, want: ports.ErrTemplateNotFound},
		{name: "code", mutate: func(r *ports.ResolveTemplateRequest) {
			r.Variables = json.RawMessage(`{"code":"secret","purpose":"LOGIN","valid_for_seconds":300}`)
		}, want: ports.ErrTemplateVariables},
		{name: "unknown field", mutate: func(r *ports.ResolveTemplateRequest) {
			r.Variables = json.RawMessage(`{"code":"123456","purpose":"LOGIN","valid_for_seconds":300,"extra":true}`)
		}, want: ports.ErrTemplateVariables},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := base
			test.mutate(&request)
			if _, err := catalog.Resolve(context.Background(), request); !errors.Is(err, test.want) {
				t.Fatalf("resolve error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestVerificationCatalogAuthorizesOnlyRegisteredTenantSenderPair(t *testing.T) {
	catalog := NewVerificationCatalog("tenant-1")
	if err := catalog.AuthorizeSender(context.Background(), "tenant-1", AINexusSenderIdentityKey); err != nil {
		t.Fatalf("authorize registered sender: %v", err)
	}
	if err := catalog.AuthorizeSender(context.Background(), "tenant-1", "other"); !errors.Is(err, ports.ErrSenderIdentityNotAllowed) {
		t.Fatalf("unknown sender error = %v", err)
	}
}
