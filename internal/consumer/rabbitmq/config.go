package rabbitmq

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	mqcontract "github.com/Zhiruosama/Email-Service/internal/messaging/rabbitmq"
)

const (
	maxConsumerLanes    = 128
	maxConsumerPrefetch = 100
)

var ErrInvalidConsumerConfig = errors.New("invalid RabbitMQ consumer configuration")

type Config struct {
	URL                   string
	ConnectionName        string
	ApplicationID         string
	Exchange              string
	Queue                 string
	RoutingKey            string
	AdditionalRoutingKeys []string
	DeadLetterExchange    string
	DeadLetterQueue       string
	DeadLetterRoutingKey  string
	ConsumerTagPrefix     string
	LaneCount             uint32
	PrefetchPerLane       uint32
	ConnectTimeout        time.Duration
	Heartbeat             time.Duration
	CloseTimeout          time.Duration
	ReconnectBase         time.Duration
	ReconnectCap          time.Duration
	TransientRequeueBase  time.Duration
	TransientRequeueCap   time.Duration
	ShutdownTimeout       time.Duration
}

func DefaultConfig(amqpURL, instanceID string) Config {
	return Config{
		URL:                  amqpURL,
		ConnectionName:       "mail-worker-" + instanceID,
		ApplicationID:        "mail-service",
		Exchange:             mqcontract.ExchangeEvents,
		Queue:                mqcontract.QueueDispatch,
		RoutingKey:           mqcontract.RoutingDispatchRequested,
		DeadLetterExchange:   mqcontract.ExchangeDead,
		DeadLetterQueue:      mqcontract.QueueDispatchDead,
		DeadLetterRoutingKey: mqcontract.RoutingDispatchDead,
		ConsumerTagPrefix:    "mail-worker-" + instanceID,
		LaneCount:            4,
		PrefetchPerLane:      1,
		ConnectTimeout:       5 * time.Second,
		Heartbeat:            10 * time.Second,
		CloseTimeout:         time.Second,
		ReconnectBase:        250 * time.Millisecond,
		ReconnectCap:         10 * time.Second,
		TransientRequeueBase: 100 * time.Millisecond,
		TransientRequeueCap:  2 * time.Second,
		ShutdownTimeout:      30 * time.Second,
	}
}

func DefaultLifecycleConfig(amqpURL, instanceID string) Config {
	config := DefaultConfig(amqpURL, instanceID)
	config.ConnectionName = "mail-notifier-" + instanceID
	config.Queue = mqcontract.QueueLifecycle
	config.RoutingKey = mqcontract.RoutingMessageAccepted
	config.AdditionalRoutingKeys = []string{mqcontract.RoutingStatusChanged}
	config.DeadLetterQueue = mqcontract.QueueLifecycleDead
	config.DeadLetterRoutingKey = mqcontract.RoutingLifecycleDead
	config.ConsumerTagPrefix = "mail-notifier-" + instanceID
	return config
}

func (c Config) Validate() error {
	if err := validateBrokerURL(c.URL); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"connection name":         c.ConnectionName,
		"application id":          c.ApplicationID,
		"exchange":                c.Exchange,
		"queue":                   c.Queue,
		"routing key":             c.RoutingKey,
		"dead letter exchange":    c.DeadLetterExchange,
		"dead letter queue":       c.DeadLetterQueue,
		"dead letter routing key": c.DeadLetterRoutingKey,
		"consumer tag prefix":     c.ConsumerTagPrefix,
	} {
		if !validAMQPName(value, 200) {
			return fmt.Errorf("%w: %s must contain 1..200 safe bytes", ErrInvalidConsumerConfig, name)
		}
	}
	if c.Exchange == c.DeadLetterExchange || c.Queue == c.DeadLetterQueue {
		return fmt.Errorf("%w: live and dead-letter topology must be distinct", ErrInvalidConsumerConfig)
	}
	routingKeys := c.routingKeys()
	seenRoutingKeys := make(map[string]struct{}, len(routingKeys))
	for _, routingKey := range routingKeys {
		if !validAMQPName(routingKey, 200) {
			return fmt.Errorf("%w: routing key must contain 1..200 safe bytes", ErrInvalidConsumerConfig)
		}
		if _, duplicate := seenRoutingKeys[routingKey]; duplicate {
			return fmt.Errorf("%w: routing keys must be unique", ErrInvalidConsumerConfig)
		}
		seenRoutingKeys[routingKey] = struct{}{}
	}
	if c.LaneCount == 0 || c.LaneCount > maxConsumerLanes {
		return fmt.Errorf("%w: lane count must be in range 1..%d", ErrInvalidConsumerConfig, maxConsumerLanes)
	}
	if c.PrefetchPerLane == 0 || c.PrefetchPerLane > maxConsumerPrefetch {
		return fmt.Errorf("%w: prefetch per lane must be in range 1..%d", ErrInvalidConsumerConfig, maxConsumerPrefetch)
	}
	if c.ConnectTimeout < 100*time.Millisecond || c.ConnectTimeout > time.Minute {
		return fmt.Errorf("%w: connect timeout must be in range 100ms..1m", ErrInvalidConsumerConfig)
	}
	if c.Heartbeat < time.Second || c.Heartbeat > 5*time.Minute {
		return fmt.Errorf("%w: heartbeat must be in range 1s..5m", ErrInvalidConsumerConfig)
	}
	if c.CloseTimeout < 100*time.Millisecond || c.CloseTimeout > 10*time.Second {
		return fmt.Errorf("%w: close timeout must be in range 100ms..10s", ErrInvalidConsumerConfig)
	}
	if err := validateBackoff("reconnect", c.ReconnectBase, c.ReconnectCap, time.Minute); err != nil {
		return err
	}
	if err := validateBackoff("transient requeue", c.TransientRequeueBase, c.TransientRequeueCap, 30*time.Second); err != nil {
		return err
	}
	if c.ShutdownTimeout < time.Second || c.ShutdownTimeout > 15*time.Minute {
		return fmt.Errorf("%w: shutdown timeout must be in range 1s..15m", ErrInvalidConsumerConfig)
	}
	return nil
}

// ValidateLifecycle additionally freezes the two V1 lifecycle bindings. A
// syntactically valid but incomplete route set would otherwise start normally
// and silently leave one class of status events unconsumed.
func (c Config) ValidateLifecycle() error {
	if err := c.Validate(); err != nil {
		return err
	}
	keys := c.routingKeys()
	if len(keys) != 2 {
		return fmt.Errorf("%w: lifecycle consumer requires exactly two V1 routing keys", ErrInvalidConsumerConfig)
	}
	want := map[string]bool{
		mqcontract.RoutingMessageAccepted: false,
		mqcontract.RoutingStatusChanged:   false,
	}
	for _, key := range keys {
		if _, known := want[key]; !known {
			return fmt.Errorf("%w: lifecycle consumer has an unsupported routing key", ErrInvalidConsumerConfig)
		}
		want[key] = true
	}
	for _, configured := range want {
		if !configured {
			return fmt.Errorf("%w: lifecycle consumer is missing a V1 routing key", ErrInvalidConsumerConfig)
		}
	}
	return nil
}

func (c Config) routingKeys() []string {
	keys := make([]string, 0, 1+len(c.AdditionalRoutingKeys))
	keys = append(keys, c.RoutingKey)
	keys = append(keys, c.AdditionalRoutingKeys...)
	return keys
}

func (c Config) clone() Config {
	cloned := c
	cloned.AdditionalRoutingKeys = append([]string(nil), c.AdditionalRoutingKeys...)
	return cloned
}

func validateBrokerURL(value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("%w: broker URL is required", ErrInvalidConsumerConfig)
	}
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "amqp" && parsed.Scheme != "amqps") || parsed.Hostname() == "" {
		return fmt.Errorf("%w: broker URL must use amqp or amqps and include a host", ErrInvalidConsumerConfig)
	}
	return nil
}

func validAMQPName(value string, maximum int) bool {
	if value == "" || len(value) > maximum || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range []byte(value) {
		if character < 0x21 || character == 0x7f {
			return false
		}
	}
	return true
}

func validateBackoff(name string, base, cap, maximum time.Duration) error {
	if base < time.Millisecond || cap < base || cap > maximum {
		return fmt.Errorf(
			"%w: %s backoff requires 1ms <= base <= cap <= %s",
			ErrInvalidConsumerConfig,
			name,
			maximum,
		)
	}
	return nil
}
