package rabbitmq

import (
	"context"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type brokerConfirmation interface {
	WaitContext(context.Context) (bool, error)
}

type brokerChannel interface {
	DeclareExchange(string) error
	DeclareQuorumQueue(string) error
	BindQueue(string, string, string) error
	EnableConfirms() error
	NotifyReturns(chan amqp.Return) <-chan amqp.Return
	Publish(
		context.Context,
		string,
		string,
		amqp.Publishing,
	) (brokerConfirmation, error)
	IsClosed() bool
	Close() error
}

type brokerConnection interface {
	OpenChannel() (brokerChannel, error)
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
			if deadlineErr := transport.SetDeadline(deadline); deadlineErr != nil {
				_ = transport.Close()
				return nil, deadlineErr
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
	return &amqpChannel{channel: channel}, nil
}

func (c *amqpConnection) IsClosed() bool {
	return c.connection.IsClosed()
}

func (c *amqpConnection) CloseDeadline(deadline time.Time) error {
	return c.connection.CloseDeadline(deadline)
}

type amqpChannel struct {
	channel *amqp.Channel
}

func (c *amqpChannel) DeclareExchange(name string) error {
	return c.channel.ExchangeDeclare(name, "topic", true, false, false, false, nil)
}

func (c *amqpChannel) DeclareQuorumQueue(name string) error {
	_, err := c.channel.QueueDeclare(
		name,
		true,
		false,
		false,
		false,
		amqp.Table{"x-queue-type": "quorum"},
	)
	return err
}

func (c *amqpChannel) BindQueue(queue, key, exchange string) error {
	return c.channel.QueueBind(queue, key, exchange, false, nil)
}

func (c *amqpChannel) EnableConfirms() error {
	return c.channel.Confirm(false)
}

func (c *amqpChannel) NotifyReturns(receiver chan amqp.Return) <-chan amqp.Return {
	return c.channel.NotifyReturn(receiver)
}

func (c *amqpChannel) Publish(
	ctx context.Context,
	exchange string,
	routingKey string,
	message amqp.Publishing,
) (brokerConfirmation, error) {
	return c.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		routingKey,
		true,
		false,
		message,
	)
}

func (c *amqpChannel) IsClosed() bool {
	return c.channel.IsClosed()
}

func (c *amqpChannel) Close() error {
	return c.channel.Close()
}
