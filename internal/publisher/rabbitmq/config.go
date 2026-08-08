package rabbitmq

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultExchangeName       = "mail.events.v1"
	DefaultDispatchQueueName  = "mail.dispatch.v1.q"
	DefaultLifecycleQueueName = "mail.lifecycle.v1.q"

	RoutingKeyMessageAccepted   = "mail.message.accepted.v1"
	RoutingKeyStatusChanged     = "mail.message.status.changed.v1"
	RoutingKeyDispatchRequested = "mail.message.dispatch.requested.v1"

	maxPublisherChannels = 128
)

var ErrInvalidRabbitMQConfig = errors.New("invalid RabbitMQ publisher configuration")

// QueueTopology describes one durable quorum queue and its exact topic
// bindings. Mutable limits and dead-letter policies intentionally remain
// broker policies instead of immutable client-provided x-arguments.
type QueueTopology struct {
	Name        string
	BindingKeys []string
}

// Config contains transport settings and the event-to-routing contract. URL
// may contain credentials and must never be logged as a whole.
type Config struct {
	URL             string
	ConnectionName  string
	ApplicationID   string
	Exchange        string
	ChannelPoolSize uint32
	ConnectTimeout  time.Duration
	Heartbeat       time.Duration
	CloseTimeout    time.Duration
	Routes          map[string]string
	Queues          []QueueTopology
}

func DefaultConfig(amqpURL, connectionName string) Config {
	return Config{
		URL:             amqpURL,
		ConnectionName:  connectionName,
		ApplicationID:   "mail-service",
		Exchange:        DefaultExchangeName,
		ChannelPoolSize: 8,
		ConnectTimeout:  5 * time.Second,
		Heartbeat:       10 * time.Second,
		CloseTimeout:    time.Second,
		Routes: map[string]string{
			"MESSAGE_ACCEPTED":           RoutingKeyMessageAccepted,
			"MESSAGE_STATUS_CHANGED":     RoutingKeyStatusChanged,
			"MESSAGE_DISPATCH_REQUESTED": RoutingKeyDispatchRequested,
		},
		Queues: []QueueTopology{
			{
				Name:        DefaultDispatchQueueName,
				BindingKeys: []string{RoutingKeyDispatchRequested},
			},
			{
				Name: DefaultLifecycleQueueName,
				BindingKeys: []string{
					RoutingKeyMessageAccepted,
					RoutingKeyStatusChanged,
				},
			},
		},
	}
}

func (c Config) Validate() error {
	if err := validateBrokerURL(c.URL); err != nil {
		return err
	}
	if !validAMQPName(c.ConnectionName, 128) {
		return fmt.Errorf(
			"%w: connection name must contain 1..128 safe bytes",
			ErrInvalidRabbitMQConfig,
		)
	}
	if !validAMQPName(c.ApplicationID, 128) {
		return fmt.Errorf(
			"%w: application id must contain 1..128 safe bytes",
			ErrInvalidRabbitMQConfig,
		)
	}
	if !validAMQPName(c.Exchange, 255) {
		return fmt.Errorf(
			"%w: exchange must contain 1..255 safe bytes",
			ErrInvalidRabbitMQConfig,
		)
	}
	if c.ChannelPoolSize == 0 || c.ChannelPoolSize > maxPublisherChannels {
		return fmt.Errorf(
			"%w: channel pool size must be in range 1..%d",
			ErrInvalidRabbitMQConfig,
			maxPublisherChannels,
		)
	}
	if c.ConnectTimeout < 100*time.Millisecond || c.ConnectTimeout > time.Minute {
		return fmt.Errorf(
			"%w: connect timeout must be in range 100ms..1m",
			ErrInvalidRabbitMQConfig,
		)
	}
	if c.Heartbeat < time.Second || c.Heartbeat > 5*time.Minute {
		return fmt.Errorf(
			"%w: heartbeat must be in range 1s..5m",
			ErrInvalidRabbitMQConfig,
		)
	}
	if c.CloseTimeout < 100*time.Millisecond || c.CloseTimeout > 10*time.Second {
		return fmt.Errorf(
			"%w: close timeout must be in range 100ms..10s",
			ErrInvalidRabbitMQConfig,
		)
	}
	return validateRoutingTopology(c.Routes, c.Queues)
}

func (c Config) clone() Config {
	cloned := c
	cloned.Routes = make(map[string]string, len(c.Routes))
	for eventType, routingKey := range c.Routes {
		cloned.Routes[eventType] = routingKey
	}
	cloned.Queues = make([]QueueTopology, len(c.Queues))
	for index, queue := range c.Queues {
		cloned.Queues[index] = QueueTopology{
			Name:        queue.Name,
			BindingKeys: append([]string(nil), queue.BindingKeys...),
		}
	}
	return cloned
}

func validateBrokerURL(value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("%w: broker URL is required", ErrInvalidRabbitMQConfig)
	}
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "amqp" && parsed.Scheme != "amqps") || parsed.Hostname() == "" {
		return fmt.Errorf(
			"%w: broker URL must use amqp or amqps and include a host",
			ErrInvalidRabbitMQConfig,
		)
	}
	return nil
}

func validateRoutingTopology(routes map[string]string, queues []QueueTopology) error {
	if len(routes) == 0 {
		return fmt.Errorf("%w: at least one event route is required", ErrInvalidRabbitMQConfig)
	}
	if len(queues) == 0 {
		return fmt.Errorf("%w: at least one queue is required", ErrInvalidRabbitMQConfig)
	}

	boundKeys := make(map[string]struct{})
	queueNames := make(map[string]struct{}, len(queues))
	for _, queue := range queues {
		if !validAMQPName(queue.Name, 255) {
			return fmt.Errorf(
				"%w: queue name must contain 1..255 safe bytes",
				ErrInvalidRabbitMQConfig,
			)
		}
		if _, duplicate := queueNames[queue.Name]; duplicate {
			return fmt.Errorf("%w: duplicate queue %q", ErrInvalidRabbitMQConfig, queue.Name)
		}
		queueNames[queue.Name] = struct{}{}
		if len(queue.BindingKeys) == 0 {
			return fmt.Errorf(
				"%w: queue %q must have at least one binding",
				ErrInvalidRabbitMQConfig,
				queue.Name,
			)
		}
		for _, bindingKey := range queue.BindingKeys {
			if !validRoutingKey(bindingKey) {
				return fmt.Errorf("%w: invalid binding key", ErrInvalidRabbitMQConfig)
			}
			boundKeys[bindingKey] = struct{}{}
		}
	}

	for eventType, routingKey := range routes {
		if !validEventType(eventType) {
			return fmt.Errorf("%w: invalid event type route", ErrInvalidRabbitMQConfig)
		}
		if !validRoutingKey(routingKey) {
			return fmt.Errorf(
				"%w: event %q has an invalid routing key",
				ErrInvalidRabbitMQConfig,
				eventType,
			)
		}
		if _, bound := boundKeys[routingKey]; !bound {
			return fmt.Errorf(
				"%w: event %q routes to an unbound key",
				ErrInvalidRabbitMQConfig,
				eventType,
			)
		}
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

func validEventType(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range []byte(value) {
		if (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '_' || character == '.' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func validRoutingKey(value string) bool {
	if value == "" || len(value) > 255 || strings.HasPrefix(value, ".") ||
		strings.HasSuffix(value, ".") || strings.Contains(value, "..") {
		return false
	}
	for _, character := range []byte(value) {
		if (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}
