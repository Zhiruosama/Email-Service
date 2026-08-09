package bootstrap

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestGRPCEndpointServesHealthAndStops(t *testing.T) {
	t.Parallel()
	endpoint, err := newGRPCEndpoint("127.0.0.1:0", time.Second)
	if err != nil {
		t.Fatalf("new gRPC endpoint: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- endpoint.Run(ctx) }()

	dialCtx, dialCancel := context.WithTimeout(context.Background(), time.Second)
	defer dialCancel()
	connection, err := grpc.DialContext(
		dialCtx,
		endpoint.Address(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		cancel()
		t.Fatalf("dial health endpoint: %v", err)
	}
	client := healthpb.NewHealthClient(connection)
	response, err := client.Check(dialCtx, &healthpb.HealthCheckRequest{Service: LivenessHealthService})
	if err != nil {
		_ = connection.Close()
		cancel()
		t.Fatalf("check liveness: %v", err)
	}
	if response.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("liveness = %s", response.Status)
	}
	_ = connection.Close()
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("stop gRPC endpoint: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gRPC endpoint did not stop")
	}
}
