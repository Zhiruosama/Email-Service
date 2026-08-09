// Package fake provides a controllable in-memory email Provider. It records
// metadata-only requests and never performs network I/O.
package fake

import (
	"context"
	"sync"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

const ProviderKey = "fake"

type Handler func(context.Context, ports.ProviderRequest) ports.ProviderResult

type Provider struct {
	mu       sync.Mutex
	handler  Handler
	requests []RequestObservation
}

// RequestObservation deliberately excludes addresses and MIME bytes so the
// test Provider cannot become a second plaintext retention store.
type RequestObservation struct {
	AttemptID          string
	MessageID          string
	TenantID           string
	AttemptNumber      uint32
	DispatchGeneration uint64
	MaterialBytes      int
}

var _ ports.EmailProvider = (*Provider)(nil)

func New(handler Handler) *Provider {
	return &Provider{handler: handler}
}

func (p *Provider) Key() string { return ProviderKey }

func (p *Provider) Submit(
	ctx context.Context,
	request ports.ProviderRequest,
) ports.ProviderResult {
	p.mu.Lock()
	p.requests = append(p.requests, RequestObservation{
		AttemptID:          request.AttemptID,
		MessageID:          request.MessageID,
		TenantID:           request.TenantID,
		AttemptNumber:      request.AttemptNumber,
		DispatchGeneration: request.DispatchGeneration,
		MaterialBytes:      len(request.Material.MIMEMessage),
	})
	handler := p.handler
	p.mu.Unlock()

	if handler != nil {
		return handler(ctx, request)
	}
	return ports.ProviderResult{
		Outcome:           ports.ProviderOutcomeAccepted,
		ProviderMessageID: "fake/" + request.AttemptID,
	}
}

func (p *Provider) Requests() []RequestObservation {
	p.mu.Lock()
	defer p.mu.Unlock()
	requests := make([]RequestObservation, len(p.requests))
	copy(requests, p.requests)
	return requests
}
