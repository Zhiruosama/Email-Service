// Package bootstrap owns process-level configuration and dependency assembly.
// Application and domain packages intentionally do not read environment variables.
package bootstrap

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	deliveryapp "github.com/Zhiruosama/Email-Service/internal/application/delivery"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	consumerrabbit "github.com/Zhiruosama/Email-Service/internal/consumer/rabbitmq"
	publisherabbit "github.com/Zhiruosama/Email-Service/internal/publisher/rabbitmq"
)

const FakeProvider = "fake"

var ErrInvalidConfig = errors.New("invalid service configuration")

type DatabaseConfig struct {
	URL             string
	MinConnections  int32
	MaxConnections  int32
	ConnectTimeout  time.Duration
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

type PollConfig struct {
	IdleDelay time.Duration
	ErrorBase time.Duration
	ErrorCap  time.Duration
}

type Config struct {
	InstanceID        string
	Provider          string
	GRPCListenAddress string
	ShutdownTimeout   time.Duration
	HealthInterval    time.Duration
	HealthTimeout     time.Duration

	Database DatabaseConfig

	SchedulerBatchSize uint32
	SchedulerLoop      PollConfig

	Relay           deliveryapp.OutboxRelayConfig
	RelayLoop       PollConfig
	OutboxRetryBase time.Duration
	OutboxRetryCap  time.Duration

	Worker            deliveryapp.DispatchWorkerConfig
	DeliveryRetryBase time.Duration
	DeliveryRetryCap  time.Duration

	Publisher publisherabbit.Config
	Consumer  consumerrabbit.Config
}

type environmentLookup func(string) (string, bool)

func LoadConfig() (Config, error) {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = "local"
	}
	return loadConfig(os.LookupEnv, hostname)
}

func loadConfig(lookup environmentLookup, hostname string) (Config, error) {
	databaseURL, err := requiredEnvironment(lookup, "DATABASE_URL")
	if err != nil {
		return Config{}, err
	}
	rabbitURL, err := requiredEnvironment(lookup, "RABBITMQ_URL")
	if err != nil {
		return Config{}, err
	}
	provider, err := requiredEnvironment(lookup, "MAIL_PROVIDER")
	if err != nil {
		return Config{}, err
	}
	instanceID := environmentOr(lookup, "MAIL_INSTANCE_ID", hostname)
	config := DefaultConfig(databaseURL, rabbitURL, instanceID, provider)
	config.GRPCListenAddress = environmentOr(lookup, "MAIL_GRPC_LISTEN_ADDRESS", config.GRPCListenAddress)

	if err := applyEnvironmentOverrides(&config, lookup); err != nil {
		return Config{}, err
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// DefaultConfig returns the safe runtime baseline before environment-specific
// tuning. Callers must still call Validate after making overrides.
func DefaultConfig(databaseURL, rabbitURL, instanceID, provider string) Config {
	return Config{
		InstanceID:        instanceID,
		Provider:          provider,
		GRPCListenAddress: ":8080",
		ShutdownTimeout:   45 * time.Second,
		HealthInterval:    2 * time.Second,
		HealthTimeout:     time.Second,
		Database: DatabaseConfig{
			URL:             databaseURL,
			MinConnections:  2,
			MaxConnections:  20,
			ConnectTimeout:  5 * time.Second,
			MaxConnLifetime: 30 * time.Minute,
			MaxConnIdleTime: 5 * time.Minute,
		},
		SchedulerBatchSize: 100,
		SchedulerLoop: PollConfig{
			IdleDelay: 250 * time.Millisecond,
			ErrorBase: 250 * time.Millisecond,
			ErrorCap:  10 * time.Second,
		},
		Relay: deliveryapp.OutboxRelayConfig{
			InstanceID:         instanceID,
			BatchSize:          100,
			PublishConcurrency: 8,
			LeaseDuration:      30 * time.Second,
			PublishTimeout:     5 * time.Second,
			MaxAttempts:        20,
		},
		RelayLoop: PollConfig{
			IdleDelay: 100 * time.Millisecond,
			ErrorBase: 250 * time.Millisecond,
			ErrorCap:  10 * time.Second,
		},
		OutboxRetryBase: time.Second,
		OutboxRetryCap:  5 * time.Minute,
		Worker: deliveryapp.DispatchWorkerConfig{
			ProviderTimeout: 10 * time.Second,
			FinalizeTimeout: 5 * time.Second,
		},
		DeliveryRetryBase: 5 * time.Second,
		DeliveryRetryCap:  5 * time.Minute,
		Publisher:         publisherabbit.DefaultConfig(rabbitURL, "mail-publisher-"+instanceID),
		Consumer:          consumerrabbit.DefaultConfig(rabbitURL, instanceID),
	}
}

func (c Config) Validate() error {
	if !validInstanceID(c.InstanceID) {
		return invalidConfig("MAIL_INSTANCE_ID must contain 1..200 safe bytes")
	}
	if c.Provider != FakeProvider {
		return invalidConfig("MAIL_PROVIDER must be explicitly set to fake in this stage")
	}
	if err := validateListenAddress(c.GRPCListenAddress); err != nil {
		return err
	}
	if c.ShutdownTimeout < time.Second || c.ShutdownTimeout > 15*time.Minute {
		return invalidConfig("MAIL_SHUTDOWN_TIMEOUT must be in range 1s..15m")
	}
	if c.HealthInterval < 100*time.Millisecond || c.HealthInterval > time.Minute {
		return invalidConfig("MAIL_HEALTH_INTERVAL must be in range 100ms..1m")
	}
	if c.HealthTimeout < 100*time.Millisecond || c.HealthTimeout > c.HealthInterval {
		return invalidConfig("MAIL_HEALTH_TIMEOUT must be in range 100ms..health interval")
	}
	if err := c.Database.Validate(); err != nil {
		return err
	}
	if c.SchedulerBatchSize == 0 || c.SchedulerBatchSize > ports.MaxDueMessageBatchSize {
		return invalidConfig("MAIL_SCHEDULER_BATCH_SIZE is outside the supported range")
	}
	if err := c.SchedulerLoop.Validate("scheduler"); err != nil {
		return err
	}
	if err := c.Relay.Validate(); err != nil {
		return fmt.Errorf("%w: relay configuration rejected", ErrInvalidConfig)
	}
	if err := c.RelayLoop.Validate("relay"); err != nil {
		return err
	}
	if _, err := deliveryapp.NewFullJitterBackoff(c.OutboxRetryBase, c.OutboxRetryCap); err != nil {
		return invalidConfig("outbox retry bounds are invalid")
	}
	if err := c.Worker.Validate(); err != nil {
		return fmt.Errorf("%w: worker configuration rejected", ErrInvalidConfig)
	}
	if _, err := deliveryapp.NewDeliveryFullJitterBackoff(c.DeliveryRetryBase, c.DeliveryRetryCap); err != nil {
		return invalidConfig("delivery retry bounds are invalid")
	}
	if err := c.Publisher.Validate(); err != nil {
		return fmt.Errorf("%w: RabbitMQ publisher configuration rejected", ErrInvalidConfig)
	}
	if err := c.Consumer.Validate(); err != nil {
		return fmt.Errorf("%w: RabbitMQ consumer configuration rejected", ErrInvalidConfig)
	}
	if c.ShutdownTimeout <= c.Consumer.ShutdownTimeout {
		return invalidConfig("MAIL_SHUTDOWN_TIMEOUT must exceed consumer shutdown timeout")
	}
	return nil
}

func (c DatabaseConfig) Validate() error {
	if c.URL == "" || strings.TrimSpace(c.URL) != c.URL || strings.ContainsAny(c.URL, "\r\n") {
		return invalidConfig("DATABASE_URL is required")
	}
	parsed, err := url.Parse(c.URL)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Hostname() == "" {
		return invalidConfig("DATABASE_URL must be a PostgreSQL URL with a host")
	}
	if c.MinConnections < 0 || c.MaxConnections < 1 || c.MinConnections > c.MaxConnections ||
		c.MaxConnections > 1000 {
		return invalidConfig("database connection bounds are invalid")
	}
	if c.ConnectTimeout < 100*time.Millisecond || c.ConnectTimeout > time.Minute {
		return invalidConfig("database connect timeout must be in range 100ms..1m")
	}
	if c.MaxConnLifetime < time.Minute || c.MaxConnLifetime > 24*time.Hour {
		return invalidConfig("database connection lifetime must be in range 1m..24h")
	}
	if c.MaxConnIdleTime < time.Second || c.MaxConnIdleTime > c.MaxConnLifetime {
		return invalidConfig("database idle time must be in range 1s..connection lifetime")
	}
	return nil
}

func (c PollConfig) Validate(name string) error {
	if c.IdleDelay < time.Millisecond || c.IdleDelay > time.Minute {
		return invalidConfig(name + " idle delay must be in range 1ms..1m")
	}
	if c.ErrorBase < time.Millisecond || c.ErrorCap < c.ErrorBase || c.ErrorCap > time.Minute {
		return invalidConfig(name + " error backoff must satisfy 1ms <= base <= cap <= 1m")
	}
	return nil
}

func applyEnvironmentOverrides(config *Config, lookup environmentLookup) error {
	durations := []struct {
		name   string
		target *time.Duration
	}{
		{"MAIL_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout},
		{"MAIL_HEALTH_INTERVAL", &config.HealthInterval},
		{"MAIL_HEALTH_TIMEOUT", &config.HealthTimeout},
		{"MAIL_DATABASE_CONNECT_TIMEOUT", &config.Database.ConnectTimeout},
		{"MAIL_DATABASE_MAX_CONN_LIFETIME", &config.Database.MaxConnLifetime},
		{"MAIL_DATABASE_MAX_CONN_IDLE_TIME", &config.Database.MaxConnIdleTime},
		{"MAIL_SCHEDULER_IDLE_DELAY", &config.SchedulerLoop.IdleDelay},
		{"MAIL_SCHEDULER_ERROR_BASE", &config.SchedulerLoop.ErrorBase},
		{"MAIL_SCHEDULER_ERROR_CAP", &config.SchedulerLoop.ErrorCap},
		{"MAIL_RELAY_LEASE_DURATION", &config.Relay.LeaseDuration},
		{"MAIL_RELAY_PUBLISH_TIMEOUT", &config.Relay.PublishTimeout},
		{"MAIL_RELAY_IDLE_DELAY", &config.RelayLoop.IdleDelay},
		{"MAIL_RELAY_ERROR_BASE", &config.RelayLoop.ErrorBase},
		{"MAIL_RELAY_ERROR_CAP", &config.RelayLoop.ErrorCap},
		{"MAIL_OUTBOX_RETRY_BASE", &config.OutboxRetryBase},
		{"MAIL_OUTBOX_RETRY_CAP", &config.OutboxRetryCap},
		{"MAIL_PROVIDER_TIMEOUT", &config.Worker.ProviderTimeout},
		{"MAIL_FINALIZE_TIMEOUT", &config.Worker.FinalizeTimeout},
		{"MAIL_DELIVERY_RETRY_BASE", &config.DeliveryRetryBase},
		{"MAIL_DELIVERY_RETRY_CAP", &config.DeliveryRetryCap},
		{"MAIL_CONSUMER_SHUTDOWN_TIMEOUT", &config.Consumer.ShutdownTimeout},
	}
	for _, field := range durations {
		if err := overrideDuration(lookup, field.name, field.target); err != nil {
			return err
		}
	}

	uint32s := []struct {
		name   string
		target *uint32
	}{
		{"MAIL_SCHEDULER_BATCH_SIZE", &config.SchedulerBatchSize},
		{"MAIL_RELAY_BATCH_SIZE", &config.Relay.BatchSize},
		{"MAIL_RELAY_PUBLISH_CONCURRENCY", &config.Relay.PublishConcurrency},
		{"MAIL_RELAY_MAX_ATTEMPTS", &config.Relay.MaxAttempts},
		{"MAIL_PUBLISHER_CHANNELS", &config.Publisher.ChannelPoolSize},
		{"MAIL_CONSUMER_LANES", &config.Consumer.LaneCount},
		{"MAIL_CONSUMER_PREFETCH", &config.Consumer.PrefetchPerLane},
	}
	for _, field := range uint32s {
		if err := overrideUint32(lookup, field.name, field.target); err != nil {
			return err
		}
	}

	if err := overrideInt32(lookup, "MAIL_DATABASE_MIN_CONNS", &config.Database.MinConnections); err != nil {
		return err
	}
	if err := overrideInt32(lookup, "MAIL_DATABASE_MAX_CONNS", &config.Database.MaxConnections); err != nil {
		return err
	}
	return nil
}

func requiredEnvironment(lookup environmentLookup, name string) (string, error) {
	value, exists := lookup(name)
	if !exists || value == "" {
		return "", invalidConfig(name + " is required")
	}
	return value, nil
}

func environmentOr(lookup environmentLookup, name, fallback string) string {
	if value, exists := lookup(name); exists {
		return value
	}
	return fallback
}

func overrideDuration(lookup environmentLookup, name string, target *time.Duration) error {
	value, exists := lookup(name)
	if !exists {
		return nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return invalidConfig(name + " must be a Go duration")
	}
	*target = parsed
	return nil
}

func overrideUint32(lookup environmentLookup, name string, target *uint32) error {
	value, exists := lookup(name)
	if !exists {
		return nil
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return invalidConfig(name + " must be an unsigned integer")
	}
	*target = uint32(parsed)
	return nil
}

func overrideInt32(lookup environmentLookup, name string, target *int32) error {
	value, exists := lookup(name)
	if !exists {
		return nil
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return invalidConfig(name + " must be a signed integer")
	}
	*target = int32(parsed)
	return nil
}

func validInstanceID(value string) bool {
	if value == "" || len(value) > 200 || strings.TrimSpace(value) != value ||
		strings.ContainsAny(value, "\r\n/") {
		return false
	}
	return true
}

func validateListenAddress(value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n") {
		return invalidConfig("MAIL_GRPC_LISTEN_ADDRESS is invalid")
	}
	_, port, err := net.SplitHostPort(value)
	if err != nil {
		return invalidConfig("MAIL_GRPC_LISTEN_ADDRESS must be host:port")
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsedPort > 65535 {
		return invalidConfig("MAIL_GRPC_LISTEN_ADDRESS has an invalid port")
	}
	return nil
}

func invalidConfig(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, message)
}
