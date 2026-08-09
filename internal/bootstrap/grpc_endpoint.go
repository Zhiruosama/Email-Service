package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	OverallHealthService  = ""
	LivenessHealthService = "mailservice.liveness.v1"
	WorkerHealthService   = "mailservice.worker.v1"
)

type grpcEndpoint struct {
	server          *grpc.Server
	health          *health.Server
	listener        net.Listener
	gracefulTimeout time.Duration
	closeOnce       sync.Once
}

type grpcEndpointConfig struct {
	serverOptions []grpc.ServerOption
	registrations []func(grpc.ServiceRegistrar)
}

type grpcEndpointOption func(*grpcEndpointConfig)

func withUnaryInterceptor(interceptor grpc.UnaryServerInterceptor) grpcEndpointOption {
	return func(config *grpcEndpointConfig) {
		config.serverOptions = append(config.serverOptions, grpc.UnaryInterceptor(interceptor))
	}
}

func withServiceRegistration(register func(grpc.ServiceRegistrar)) grpcEndpointOption {
	return func(config *grpcEndpointConfig) {
		config.registrations = append(config.registrations, register)
	}
}

func newGRPCEndpoint(
	address string,
	gracefulTimeout time.Duration,
	options ...grpcEndpointOption,
) (*grpcEndpoint, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen on configured gRPC address: %w", err)
	}
	healthServer := health.NewServer()
	healthServer.SetServingStatus(LivenessHealthService, healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(OverallHealthService, healthpb.HealthCheckResponse_NOT_SERVING)
	healthServer.SetServingStatus(WorkerHealthService, healthpb.HealthCheckResponse_NOT_SERVING)
	configuration := grpcEndpointConfig{}
	for _, option := range options {
		if option != nil {
			option(&configuration)
		}
	}
	server := grpc.NewServer(configuration.serverOptions...)
	healthpb.RegisterHealthServer(server, healthServer)
	for _, register := range configuration.registrations {
		register(server)
	}
	return &grpcEndpoint{
		server:          server,
		health:          healthServer,
		listener:        listener,
		gracefulTimeout: gracefulTimeout,
	}, nil
}

func (e *grpcEndpoint) Run(ctx context.Context) error {
	serveResult := make(chan error, 1)
	go func() { serveResult <- e.server.Serve(e.listener) }()
	select {
	case err := <-serveResult:
		if err == nil || errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return fmt.Errorf("gRPC server stopped: %w", err)
	case <-ctx.Done():
		e.health.Shutdown()
		gracefulDone := make(chan struct{})
		go func() {
			e.server.GracefulStop()
			close(gracefulDone)
		}()
		timer := time.NewTimer(e.gracefulTimeout)
		defer timer.Stop()
		select {
		case <-gracefulDone:
		case <-timer.C:
			e.server.Stop()
			<-gracefulDone
		}
		return nil
	}
}

func (e *grpcEndpoint) Close() error {
	var closeErr error
	e.closeOnce.Do(func() {
		e.health.Shutdown()
		e.server.Stop()
		closeErr = e.listener.Close()
		if errors.Is(closeErr, net.ErrClosed) {
			closeErr = nil
		}
	})
	return closeErr
}

func (e *grpcEndpoint) Address() string { return e.listener.Addr().String() }
