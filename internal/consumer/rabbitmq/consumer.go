// Package rabbitmq consumes dispatch commands with manual acknowledgements,
// bounded prefetch, independent channel lanes, and reconnect supervision.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	deliveryapp "github.com/Zhiruosama/Email-Service/internal/application/delivery"
	notificationapp "github.com/Zhiruosama/Email-Service/internal/application/notification"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrConsumerSession  = errors.New("RabbitMQ consumer session failure")
	ErrConsumerChannel  = errors.New("RabbitMQ consumer channel failure")
	ErrConsumerAck      = errors.New("RabbitMQ consumer acknowledgement failure")
	ErrConsumerShutdown = errors.New("RabbitMQ consumer shutdown timeout")
)

type DispatchProcessor interface {
	Process(context.Context, deliveryapp.DispatchCommand) (deliveryapp.DispatchResult, error)
}

type NotificationProcessor interface {
	Process(context.Context, notificationapp.Command) (notificationapp.Result, error)
}

type deliveryHandler func(context.Context, amqp.Delivery) error

type Consumer struct {
	config                Config
	processor             DispatchProcessor
	notificationProcessor NotificationProcessor
	handle                deliveryHandler
	dial                  connectionFactory
	ready                 atomic.Bool
}

// Ready reports whether the current connection has declared topology and
// started all configured consumer lanes. It becomes false between reconnects
// and during shutdown.
func (c *Consumer) Ready() bool { return c.ready.Load() }

func New(config Config, processor DispatchProcessor) (*Consumer, error) {
	return newConsumer(config, processor, dialAMQP)
}

func newConsumer(
	config Config,
	processor DispatchProcessor,
	dial connectionFactory,
) (*Consumer, error) {
	if processor == nil {
		panic("rabbitmq consumer: nil dispatch processor")
	}
	if dial == nil {
		panic("rabbitmq consumer: nil connection factory")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	consumer := &Consumer{config: config.clone(), processor: processor, dial: dial}
	consumer.handle = consumer.handleDelivery
	return consumer, nil
}

func NewLifecycle(config Config, processor NotificationProcessor) (*Consumer, error) {
	return newLifecycleConsumer(config, processor, dialAMQP)
}

func newLifecycleConsumer(
	config Config,
	processor NotificationProcessor,
	dial connectionFactory,
) (*Consumer, error) {
	if processor == nil {
		panic("rabbitmq consumer: nil notification processor")
	}
	if dial == nil {
		panic("rabbitmq consumer: nil connection factory")
	}
	if err := config.ValidateLifecycle(); err != nil {
		return nil, err
	}
	consumer := &Consumer{
		config:                config.clone(),
		notificationProcessor: processor,
		dial:                  dial,
	}
	consumer.handle = consumer.handleLifecycleDelivery
	return consumer, nil
}

// Run keeps reconnecting until ctx is cancelled. A graceful cancellation
// stops new deliveries, gives active handlers ShutdownTimeout to finish their
// bounded finalization, and only then closes the AMQP connection.
func (c *Consumer) Run(ctx context.Context) error {
	c.ready.Store(false)
	defer c.ready.Store(false)
	var reconnectAttempt uint32
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		connection, err := c.dial(ctx, c.config)
		if err != nil {
			reconnectAttempt++
			if !waitContext(ctx, exponentialFullJitter(
				c.config.ReconnectBase,
				c.config.ReconnectCap,
				reconnectAttempt,
			)) {
				return nil
			}
			continue
		}

		sessionStarted := time.Now()
		err = c.runSession(ctx, connection)
		if ctx.Err() != nil {
			if errors.Is(err, ErrConsumerShutdown) {
				return err
			}
			return nil
		}
		// A connection that stayed healthy for at least one heartbeat interval
		// proves recovery. A connection that immediately fails topology/session
		// setup must keep increasing backoff instead of reconnecting in a hot loop.
		if time.Since(sessionStarted) >= c.config.Heartbeat {
			reconnectAttempt = 0
		}
		reconnectAttempt++
		if !waitContext(ctx, exponentialFullJitter(
			c.config.ReconnectBase,
			c.config.ReconnectCap,
			reconnectAttempt,
		)) {
			return nil
		}
	}
}

func (c *Consumer) runSession(ctx context.Context, connection brokerConnection) error {
	if err := declareConsumerTopology(connection, c.config); err != nil {
		_ = connection.CloseDeadline(time.Now().Add(c.config.CloseTimeout))
		return fmt.Errorf("%w: declare topology: %v", ErrConsumerSession, err)
	}

	sessionCtx, cancelSession := context.WithCancel(ctx)
	laneResults := make(chan error, c.config.LaneCount)
	var lanes sync.WaitGroup
	for lane := uint32(1); lane <= c.config.LaneCount; lane++ {
		lanes.Add(1)
		go func(laneNumber uint32) {
			defer lanes.Done()
			laneResults <- c.runLane(sessionCtx, connection, laneNumber)
		}(lane)
	}
	allLanesDone := make(chan struct{})
	go func() {
		lanes.Wait()
		close(allLanesDone)
	}()
	c.ready.Store(true)
	defer c.ready.Store(false)

	connectionClosed := connection.NotifyClose(make(chan *amqp.Error, 1))
	var sessionErr error
	select {
	case <-ctx.Done():
		cancelSession()
		shutdownTimer := time.NewTimer(c.config.ShutdownTimeout)
		select {
		case <-allLanesDone:
			if !shutdownTimer.Stop() {
				<-shutdownTimer.C
			}
		case <-shutdownTimer.C:
			sessionErr = ErrConsumerShutdown
		}
	case brokerErr, open := <-connectionClosed:
		if open && brokerErr != nil {
			sessionErr = fmt.Errorf("%w: connection closed: %s", ErrConsumerSession, brokerErr.Reason)
		} else {
			sessionErr = fmt.Errorf("%w: connection closed", ErrConsumerSession)
		}
		cancelSession()
	case laneErr := <-laneResults:
		if laneErr != nil {
			sessionErr = laneErr
		} else {
			sessionErr = fmt.Errorf("%w: lane exited unexpectedly", ErrConsumerSession)
		}
		cancelSession()
	}

	_ = connection.CloseDeadline(time.Now().Add(c.config.CloseTimeout))
	if sessionErr != nil && errors.Is(sessionErr, ErrConsumerShutdown) {
		return sessionErr
	}
	select {
	case <-allLanesDone:
	case <-time.After(c.config.CloseTimeout):
	}
	return sessionErr
}

func (c *Consumer) runLane(
	ctx context.Context,
	connection brokerConnection,
	laneNumber uint32,
) error {
	var restartAttempt uint32
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		channel, err := connection.OpenChannel()
		if err != nil {
			if connection.IsClosed() {
				return fmt.Errorf("%w: connection unavailable", ErrConsumerChannel)
			}
			restartAttempt++
			if !waitContext(ctx, exponentialFullJitter(
				c.config.ReconnectBase,
				c.config.ReconnectCap,
				restartAttempt,
			)) {
				return nil
			}
			continue
		}

		channelStarted := time.Now()
		_ = c.consumeLane(ctx, channel, laneNumber)
		if ctx.Err() != nil {
			// ConsumeWithContext already issues basic.cancel. Avoid racing that
			// handshake with Channel.Close; runSession closes the connection once
			// every lane has returned.
			return nil
		}
		_ = channel.Close()
		if connection.IsClosed() {
			return fmt.Errorf("%w: connection closed", ErrConsumerChannel)
		}
		if time.Since(channelStarted) >= c.config.Heartbeat {
			restartAttempt = 0
		}
		restartAttempt++
		if !waitContext(ctx, exponentialFullJitter(
			c.config.ReconnectBase,
			c.config.ReconnectCap,
			restartAttempt,
		)) {
			return nil
		}
	}
}

func (c *Consumer) consumeLane(
	ctx context.Context,
	channel brokerChannel,
	laneNumber uint32,
) error {
	if err := channel.Qos(int(c.config.PrefetchPerLane), 0, false); err != nil {
		return fmt.Errorf("%w: configure prefetch: %v", ErrConsumerChannel, err)
	}
	consumerTag := fmt.Sprintf("%s-%03d", c.config.ConsumerTagPrefix, laneNumber)
	deliveries, err := channel.ConsumeWithContext(
		ctx,
		c.config.Queue,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("%w: begin consume: %v", ErrConsumerChannel, err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, open := <-deliveries:
			if !open {
				return fmt.Errorf("%w: delivery stream closed", ErrConsumerChannel)
			}
			if err := c.handle(ctx, delivery); err != nil {
				return err
			}
		}
	}
}

func (c *Consumer) handleLifecycleDelivery(ctx context.Context, delivery amqp.Delivery) error {
	command, err := ParseLifecycleDelivery(delivery, c.config)
	if err != nil {
		return deadLetterDelivery(delivery, "poison lifecycle delivery")
	}

	_, err = c.notificationProcessor.Process(ctx, command)
	if err == nil {
		return acknowledgeDelivery(delivery, "processed lifecycle delivery")
	}
	if notificationapp.ClassifyError(err) == notificationapp.ErrorPoison {
		return deadLetterDelivery(delivery, "poison notification command")
	}
	return c.requeueTransient(ctx, delivery, "transient notification failure")
}

func (c *Consumer) handleDelivery(ctx context.Context, delivery amqp.Delivery) error {
	command, err := ParseDispatchDelivery(delivery, c.config)
	if err != nil {
		if nackErr := delivery.Nack(false, false); nackErr != nil {
			return fmt.Errorf("%w: dead-letter poison delivery: %v", ErrConsumerAck, nackErr)
		}
		return nil
	}

	_, err = c.processor.Process(ctx, command)
	if err == nil {
		if ackErr := delivery.Ack(false); ackErr != nil {
			return fmt.Errorf("%w: acknowledge processed delivery: %v", ErrConsumerAck, ackErr)
		}
		return nil
	}
	if deliveryapp.ClassifyDispatchError(err) == deliveryapp.DispatchErrorPoison {
		// basic.nack is a regular return in RabbitMQ 4.3. With requeue=false
		// it dead-letters immediately without entering the delayed retry path
		// reserved for failed basic.reject deliveries.
		if nackErr := delivery.Nack(false, false); nackErr != nil {
			return fmt.Errorf("%w: dead-letter poison command: %v", ErrConsumerAck, nackErr)
		}
		return nil
	}

	delay := boundedJitter(c.config.TransientRequeueBase, c.config.TransientRequeueCap)
	if !waitContext(ctx, delay) {
		return ctx.Err()
	}
	// RabbitMQ 4.3 quorum queues classify basic.reject as a failed return. It
	// therefore increments x-delivery-count and participates in delayed retry
	// plus delivery-limit. basic.nack(requeue=true) is only a regular return and
	// could otherwise retry forever without consuming the delivery budget.
	if rejectErr := delivery.Reject(true); rejectErr != nil {
		return fmt.Errorf("%w: requeue transient failure: %v", ErrConsumerAck, rejectErr)
	}
	return nil
}

func acknowledgeDelivery(delivery amqp.Delivery, operation string) error {
	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("%w: acknowledge %s: %v", ErrConsumerAck, operation, err)
	}
	return nil
}

func deadLetterDelivery(delivery amqp.Delivery, operation string) error {
	if err := delivery.Nack(false, false); err != nil {
		return fmt.Errorf("%w: dead-letter %s: %v", ErrConsumerAck, operation, err)
	}
	return nil
}

func (c *Consumer) requeueTransient(
	ctx context.Context,
	delivery amqp.Delivery,
	operation string,
) error {
	delay := boundedJitter(c.config.TransientRequeueBase, c.config.TransientRequeueCap)
	if !waitContext(ctx, delay) {
		return ctx.Err()
	}
	if err := delivery.Reject(true); err != nil {
		return fmt.Errorf("%w: requeue %s: %v", ErrConsumerAck, operation, err)
	}
	return nil
}

func declareConsumerTopology(connection brokerConnection, config Config) error {
	channel, err := connection.OpenChannel()
	if err != nil {
		return err
	}
	defer channel.Close()

	if err := channel.ExchangeDeclare(
		config.DeadLetterExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}
	if _, err := channel.QueueDeclare(
		config.DeadLetterQueue,
		true,
		false,
		false,
		false,
		amqp.Table{"x-queue-type": "quorum"},
	); err != nil {
		return err
	}
	if err := channel.QueueBind(
		config.DeadLetterQueue,
		config.DeadLetterRoutingKey,
		config.DeadLetterExchange,
		false,
		nil,
	); err != nil {
		return err
	}

	if err := channel.ExchangeDeclare(
		config.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}
	if _, err := channel.QueueDeclare(
		config.Queue,
		true,
		false,
		false,
		false,
		amqp.Table{"x-queue-type": "quorum"},
	); err != nil {
		return err
	}
	for _, routingKey := range config.routingKeys() {
		if err := channel.QueueBind(config.Queue, routingKey, config.Exchange, false, nil); err != nil {
			return err
		}
	}
	return nil
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
