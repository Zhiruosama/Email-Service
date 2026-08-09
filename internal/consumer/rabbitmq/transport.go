package rabbitmq

import (
	"context"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type brokerChannel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool) error
	ConsumeWithContext(
		context.Context,
		string,
		string,
		bool,
		bool,
		bool,
		bool,
		amqp.Table,
	) (<-chan amqp.Delivery, error)
	IsClosed() bool
	Close() error
}

type brokerConnection interface {
	OpenChannel() (brokerChannel, error)
	NotifyClose(chan *amqp.Error) <-chan *amqp.Error
	IsClosed() bool
	CloseDeadline(time.Time) error
}

type connectionFactory func(context.Context, Config) (brokerConnection, error)

type amqpConnection struct {
	connection *amqp.Connection
}

func dialAMQP(ctx context.Context, config Config) (brokerConnection, error) {
	properties := amqp.NewConnectionProperties()
	properties.SetClientConnectionName(config.ConnectionName)
	dialer := &net.Dialer{
		Timeout:   config.ConnectTimeout,
		KeepAlive: 30 * time.Second,
	}
	connection, err := amqp.DialConfig(config.URL, amqp.Config{
		Heartbeat:  config.Heartbeat,
		Locale:     "en_US",
		Properties: properties,
		Dial: func(network, address string) (net.Conn, error) {
			transport, dialErr := dialer.DialContext(ctx, network, address)
			if dialErr != nil {
				return nil, dialErr
			}
			deadline := time.Now().Add(config.ConnectTimeout)
			if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
				deadline = contextDeadline
			}
			if err := transport.SetDeadline(deadline); err != nil {
				_ = transport.Close()
				return nil, err
			}
			return transport, nil
		},
	})
	if err != nil {
		return nil, err
	}
	return &amqpConnection{connection: connection}, nil
}

func (c *amqpConnection) OpenChannel() (brokerChannel, error) {
	channel, err := c.connection.Channel()
	if err != nil {
		return nil, err
	}
	return channel, nil
}

func (c *amqpConnection) NotifyClose(receiver chan *amqp.Error) <-chan *amqp.Error {
	return c.connection.NotifyClose(receiver)
}

func (c *amqpConnection) IsClosed() bool { return c.connection.IsClosed() }

func (c *amqpConnection) CloseDeadline(deadline time.Time) error {
	return c.connection.CloseDeadline(deadline)
}
