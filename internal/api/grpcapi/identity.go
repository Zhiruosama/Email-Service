package grpcapi

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const deliveryMethodPrefix = "/mailservice.delivery.v1.DeliveryService/"

type tenantContextKey struct{}

// FixedDevelopmentTenantInterceptor is deliberately limited to the current
// insecure local stage. Production mTLS will replace it without changing
// application commands or allowing tenant_id into request messages.
func FixedDevelopmentTenantInterceptor(tenantID string) grpc.UnaryServerInterceptor {
	if _, err := uuid.Parse(tenantID); err != nil {
		panic("grpcapi: fixed development tenant id must be a UUID")
	}
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if strings.HasPrefix(info.FullMethod, deliveryMethodPrefix) {
			ctx = context.WithValue(ctx, tenantContextKey{}, tenantID)
		}
		return handler(ctx, req)
	}
}

func TenantIDFromContext(ctx context.Context) (string, error) {
	tenantID, ok := ctx.Value(tenantContextKey{}).(string)
	if !ok || tenantID == "" {
		return "", status.Error(codes.Unauthenticated, "authenticated tenant identity is required")
	}
	return tenantID, nil
}
