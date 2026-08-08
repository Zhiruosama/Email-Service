// Package fake provides a controllable, in-memory Outbox Publisher for local
// vertical slices and failure tests. It performs no network I/O.
package fake

import (
	"context"
	"sync"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

type Handler func(context.Context, ports.OutboxPublication) error

type Publisher struct {
	mu           sync.Mutex
	handler      Handler
	publications []ports.OutboxPublication
}

var _ ports.OutboxPublisher = (*Publisher)(nil)

func New(handler Handler) *Publisher {
	return &Publisher{handler: handler}
}

func (p *Publisher) Publish(
	ctx context.Context,
	publication ports.OutboxPublication,
) error {
	p.mu.Lock()
	p.publications = append(p.publications, clonePublication(publication))
	handler := p.handler
	p.mu.Unlock()
	if handler == nil {
		return nil
	}
	return handler(ctx, publication)
}

func (p *Publisher) Publications() []ports.OutboxPublication {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]ports.OutboxPublication, len(p.publications))
	for index, publication := range p.publications {
		result[index] = clonePublication(publication)
	}
	return result
}

func clonePublication(publication ports.OutboxPublication) ports.OutboxPublication {
	cloned := publication
	cloned.Event.Payload = append([]byte(nil), publication.Event.Payload...)
	return cloned
}
