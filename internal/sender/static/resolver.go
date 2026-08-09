// Package static provides a bootstrap-time sender identity registry. A
// database-backed control-plane adapter can replace it through the same port.
package static

import (
	"context"
	"fmt"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/google/uuid"
)

type Resolver struct {
	tenantID string
	identity ports.SenderIdentity
}

var _ ports.SenderIdentityResolver = (*Resolver)(nil)

func New(tenantID string, identity ports.SenderIdentity) (*Resolver, error) {
	if _, err := uuid.Parse(tenantID); err != nil {
		return nil, fmt.Errorf("%w: tenant id must be a UUID", ports.ErrInvalidSenderIdentity)
	}
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	return &Resolver{tenantID: tenantID, identity: identity}, nil
}

func (r *Resolver) ResolveSender(
	_ context.Context,
	tenantID, senderKey string,
) (ports.SenderIdentity, error) {
	if tenantID != r.tenantID || senderKey != r.identity.Key {
		return ports.SenderIdentity{}, ports.ErrSenderIdentityNotAllowed
	}
	return r.identity, nil
}
