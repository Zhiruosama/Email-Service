package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	deliveryapp "github.com/Zhiruosama/Email-Service/internal/application/delivery"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	consumerrabbit "github.com/Zhiruosama/Email-Service/internal/consumer/rabbitmq"
	providerfake "github.com/Zhiruosama/Email-Service/internal/provider/fake"
	publisherabbit "github.com/Zhiruosama/Email-Service/internal/publisher/rabbitmq"
	postgresstore "github.com/Zhiruosama/Email-Service/internal/storage/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var (
	ErrStartup       = errors.New("service startup failure")
	ErrAppAlreadyRun = errors.New("service application already run")
)

type App struct {
	config    Config
	logger    *slog.Logger
	pool      *pgxpool.Pool
	publisher *publisherabbit.Publisher
	consumer  *consumerrabbit.Consumer
	endpoint  *grpcEndpoint
	scheduler *poller
	relay     *poller
	readiness *readinessMonitor

	mu        sync.Mutex
	runCalled bool
	closeOnce sync.Once
	closeErr  error
}

func NewApp(ctx context.Context, config Config, logger *slog.Logger) (*App, error) {
	if logger == nil {
		panic("bootstrap: nil logger")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}

	pool, err := openDatabase(ctx, config)
	if err != nil {
		return nil, err
	}
	cleanupPool := true
	defer func() {
		if cleanupPool {
			pool.Close()
		}
	}()

	transactor := postgresstore.NewTransactionManager(pool)
	publisher, err := publisherabbit.New(config.Publisher)
	if err != nil {
		return nil, fmt.Errorf("%w: create RabbitMQ publisher", ErrStartup)
	}
	cleanupPublisher := true
	defer func() {
		if cleanupPublisher {
			_ = publisher.Close()
		}
	}()

	scheduler, err := deliveryapp.NewDueMessageScheduler(transactor, config.SchedulerBatchSize)
	if err != nil {
		return nil, fmt.Errorf("%w: create due-message scheduler", ErrStartup)
	}
	outboxRetry, err := deliveryapp.NewFullJitterBackoff(config.OutboxRetryBase, config.OutboxRetryCap)
	if err != nil {
		return nil, fmt.Errorf("%w: create outbox retry policy", ErrStartup)
	}
	relay, err := deliveryapp.NewOutboxRelay(transactor, publisher, outboxRetry, config.Relay)
	if err != nil {
		return nil, fmt.Errorf("%w: create outbox relay", ErrStartup)
	}
	deliveryRetry, err := deliveryapp.NewDeliveryFullJitterBackoff(
		config.DeliveryRetryBase,
		config.DeliveryRetryCap,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: create delivery retry policy", ErrStartup)
	}
	provider := providerfake.New(nil)
	worker, err := deliveryapp.NewDispatchWorker(transactor, provider, deliveryRetry, config.Worker)
	if err != nil {
		return nil, fmt.Errorf("%w: create dispatch worker", ErrStartup)
	}
	consumer, err := consumerrabbit.New(config.Consumer, worker)
	if err != nil {
		return nil, fmt.Errorf("%w: create RabbitMQ consumer", ErrStartup)
	}
	endpoint, err := newGRPCEndpoint(config.GRPCListenAddress, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("%w: create gRPC endpoint: %v", ErrStartup, err)
	}

	app := &App{
		config:    config,
		logger:    logger,
		pool:      pool,
		publisher: publisher,
		consumer:  consumer,
		endpoint:  endpoint,
	}
	app.scheduler = newPoller(
		"due_message_scheduler",
		config.SchedulerLoop,
		logger,
		func(ctx context.Context) (uint32, error) {
			result, runErr := scheduler.RunOnce(ctx)
			return result.Claimed, runErr
		},
		isFatalSchedulerError,
	)
	app.relay = newPoller(
		"outbox_relay",
		config.RelayLoop,
		logger,
		func(ctx context.Context) (uint32, error) {
			result, runErr := relay.RunOnce(ctx)
			return result.Claimed, runErr
		},
		isFatalRelayError,
	)
	app.readiness = &readinessMonitor{
		database: pool,
		consumer: consumer,
		health:   endpoint.health,
		logger:   logger,
		interval: config.HealthInterval,
		timeout:  config.HealthTimeout,
	}

	cleanupPublisher = false
	cleanupPool = false
	return app, nil
}

func (a *App) Run(ctx context.Context) error {
	a.mu.Lock()
	if a.runCalled {
		a.mu.Unlock()
		return ErrAppAlreadyRun
	}
	a.runCalled = true
	a.mu.Unlock()

	a.logger.Info(
		"mail service runtime starting",
		"instance_id", a.config.InstanceID,
		"provider", a.config.Provider,
		"grpc_address", a.endpoint.Address(),
	)
	supervisor := stagedSupervisor{
		components: []runtimeComponent{
			{name: "readiness", stage: 0, run: a.readiness.Run},
			{name: "scheduler", stage: 1, run: a.scheduler.Run},
			{name: "outbox_relay", stage: 1, run: a.relay.Run},
			{name: "rabbitmq_consumer", stage: 2, run: a.consumer.Run},
			{name: "grpc_server", stage: 3, run: a.endpoint.Run},
		},
		shutdownTimeout: a.config.ShutdownTimeout,
		beforeShutdown: func() {
			a.endpoint.health.SetServingStatus(
				OverallHealthService,
				grpc_health_v1.HealthCheckResponse_NOT_SERVING,
			)
			a.endpoint.health.SetServingStatus(
				WorkerHealthService,
				grpc_health_v1.HealthCheckResponse_NOT_SERVING,
			)
			a.logger.Info("mail service runtime draining")
		},
	}
	runErr := supervisor.Run(ctx)
	closeErr := a.Close()
	if runErr == nil && closeErr == nil {
		a.logger.Info("mail service runtime stopped")
		return nil
	}
	return errors.Join(runErr, closeErr)
}

func (a *App) Close() error {
	a.closeOnce.Do(func() {
		endpointErr := a.endpoint.Close()
		publisherErr := a.publisher.Close()
		a.pool.Close()
		a.closeErr = errors.Join(endpointErr, publisherErr)
	})
	return a.closeErr
}

func (a *App) GRPCAddress() string { return a.endpoint.Address() }

func openDatabase(ctx context.Context, config Config) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(config.Database.URL)
	if err != nil {
		return nil, fmt.Errorf("%w: parse DATABASE_URL", ErrStartup)
	}
	poolConfig.MinConns = config.Database.MinConnections
	poolConfig.MaxConns = config.Database.MaxConnections
	poolConfig.MaxConnLifetime = config.Database.MaxConnLifetime
	poolConfig.MaxConnLifetimeJitter = config.Database.MaxConnLifetime / 10
	poolConfig.MaxConnIdleTime = config.Database.MaxConnIdleTime
	poolConfig.ConnConfig.ConnectTimeout = config.Database.ConnectTimeout
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "mail-service-" + config.InstanceID

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("%w: create PostgreSQL pool", ErrStartup)
	}
	checkCtx, cancel := context.WithTimeout(ctx, config.Database.ConnectTimeout)
	err = pool.Ping(checkCtx)
	if err == nil {
		err = verifyDatabaseSchema(checkCtx, pool)
	}
	cancel()
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("%w: PostgreSQL is unavailable or migrations are incomplete", ErrStartup)
	}
	return pool, nil
}

func verifyDatabaseSchema(ctx context.Context, pool *pgxpool.Pool) error {
	const query = `
SELECT
    to_regclass('public.tenants') IS NOT NULL
    AND to_regclass('public.mail_messages') IS NOT NULL
    AND to_regclass('public.outbox_events') IS NOT NULL
	AND to_regclass('public.delivery_attempts') IS NOT NULL,
    CASE
        WHEN to_regclass('public.goose_db_version') IS NULL THEN 0
        ELSE (SELECT COALESCE(max(version_id) FILTER (WHERE is_applied), 0) FROM goose_db_version)
    END`
	var complete bool
	var version int64
	if err := pool.QueryRow(ctx, query).Scan(&complete, &version); err != nil {
		return err
	}
	if !complete || version < 3 {
		return errors.New("required schema is missing")
	}
	return nil
}

func isFatalSchedulerError(err error) bool {
	return errors.Is(err, deliveryapp.ErrSchedulerInvariant) ||
		errors.Is(err, deliveryapp.ErrMessageEventMapping) ||
		errors.Is(err, deliveryapp.ErrNoPendingMessageEvents) ||
		errors.Is(err, ports.ErrCorruptMessageRecord) ||
		errors.Is(err, ports.ErrInvalidMessageRecord) ||
		errors.Is(err, ports.ErrInvalidOutboxEvent)
}

func isFatalRelayError(err error) bool {
	return errors.Is(err, deliveryapp.ErrOutboxRelayInvariant) ||
		errors.Is(err, deliveryapp.ErrInvalidOutboxRetryDelay) ||
		errors.Is(err, ports.ErrCorruptOutboxDelivery) ||
		errors.Is(err, ports.ErrInvalidOutboxDelivery) ||
		errors.Is(err, ports.ErrInvalidOutboxEvent)
}
