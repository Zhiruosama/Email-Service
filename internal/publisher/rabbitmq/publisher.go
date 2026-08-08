// Package rabbitmq publishes transport-neutral Outbox events to RabbitMQ with
// mandatory routing, persistent delivery, and per-message publisher confirms.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ErrorCodeInvalidPublication = "RABBITMQ_INVALID_PUBLICATION"
	ErrorCodeUnsupportedEvent   = "RABBITMQ_UNSUPPORTED_EVENT"
	ErrorCodeUnavailable        = "RABBITMQ_UNAVAILABLE"
	ErrorCodeTopology           = "RABBITMQ_TOPOLOGY"
	ErrorCodePublish            = "RABBITMQ_PUBLISH"
	ErrorCodeConfirmMissing     = "RABBITMQ_CONFIRM_MISSING"
	ErrorCodeNack               = "RABBITMQ_NACK"
	ErrorCodeUnroutable         = "RABBITMQ_UNROUTABLE"
	ErrorCodeProtocol           = "RABBITMQ_PROTOCOL"
	ErrorCodeClosed             = "RABBITMQ_CLOSED"
)

var ErrRabbitMQPublisherClosed = errors.New("RabbitMQ publisher is closed")

type Publisher struct {
	config Config
	dial   connectionFactory
	now    func() time.Time
	lanes  chan *publisherLane
	done   chan struct{}

	mu         sync.Mutex
	connection brokerConnection
	closed     bool
}

type publisherLane struct {
	connection brokerConnection
	channel    brokerChannel
	returns    <-chan amqp.Return
}

var _ ports.OutboxPublisher = (*Publisher)(nil)

// New validates local configuration but intentionally does not contact the
// broker. A RabbitMQ outage must not prevent the process from starting and the
// durable PostgreSQL Outbox remains the recovery source of truth.
func New(config Config) (*Publisher, error) {
	return newPublisher(config, dialAMQP)
}

func newPublisher(config Config, dial connectionFactory) (*Publisher, error) {
	if dial == nil {
		panic("rabbitmq: nil connection factory")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	cloned := config.clone()
	publisher := &Publisher{
		config: cloned,
		dial:   dial,
		now:    time.Now,
		lanes:  make(chan *publisherLane, cloned.ChannelPoolSize),
		done:   make(chan struct{}),
	}
	for range cloned.ChannelPoolSize {
		publisher.lanes <- &publisherLane{}
	}
	return publisher, nil
}

func (p *Publisher) Publish(
	ctx context.Context,
	publication ports.OutboxPublication,
) error {
	if err := validatePublication(publication); err != nil {
		return publishError(ErrorCodeInvalidPublication, false, err)
	}
	routingKey, supported := p.config.Routes[publication.Event.EventType]
	if !supported {
		return publishError(
			ErrorCodeUnsupportedEvent,
			false,
			fmt.Errorf("event type %q has no route", publication.Event.EventType),
		)
	}

	lane, err := p.borrowLane(ctx)
	if err != nil {
		return err
	}
	defer func() {
		p.lanes <- lane
	}()

	if err := p.ensureLane(ctx, lane); err != nil {
		return err
	}
	message := p.makePublishing(publication)
	confirmation, err := p.publishBounded(
		ctx,
		lane,
		routingKey,
		message,
	)
	if err != nil {
		return err
	}

	acknowledged, err := confirmation.WaitContext(ctx)
	if err != nil {
		p.abortConnection(lane.connection)
		p.resetLane(lane)
		return err
	}
	if !acknowledged {
		return publishError(ErrorCodeNack, true, errors.New("broker negatively acknowledged publish"))
	}

	select {
	case returned, open := <-lane.returns:
		if !open {
			// An ACK is definitive for a routed message. RabbitMQ dispatches a
			// mandatory Return before the corresponding ACK, so a subsequently
			// closed Return listener does not revoke that ACK.
			return nil
		}
		if returned.MessageId != publication.Event.ID {
			p.abortConnection(lane.connection)
			p.resetLane(lane)
			return publishError(
				ErrorCodeProtocol,
				true,
				fmt.Errorf(
					"mandatory return message id %q does not match publication",
					returned.MessageId,
				),
			)
		}
		return publishError(
			ErrorCodeUnroutable,
			false,
			fmt.Errorf(
				"broker returned code %d for exchange %q and routing key %q",
				returned.ReplyCode,
				returned.Exchange,
				returned.RoutingKey,
			),
		)
	default:
		return nil
	}
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.done)
	connection := p.connection
	p.connection = nil
	p.mu.Unlock()
	if connection == nil || connection.IsClosed() {
		return nil
	}
	return connection.CloseDeadline(time.Now().Add(p.config.CloseTimeout))
}

func (p *Publisher) borrowLane(ctx context.Context) (*publisherLane, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.done:
		return nil, publishError(ErrorCodeClosed, true, ErrRabbitMQPublisherClosed)
	case lane := <-p.lanes:
		return lane, nil
	}
}

func (p *Publisher) ensureLane(ctx context.Context, lane *publisherLane) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return publishError(ErrorCodeClosed, true, ErrRabbitMQPublisherClosed)
	}

	connection := p.connection
	if connection == nil || connection.IsClosed() {
		connected, err := p.dial(ctx, p.config)
		if err != nil {
			return publishError(ErrorCodeUnavailable, true, err)
		}
		if err := declareTopology(connected, p.config); err != nil {
			_ = connected.CloseDeadline(time.Now().Add(p.config.CloseTimeout))
			return publishError(ErrorCodeTopology, true, err)
		}
		p.connection = connected
		connection = connected
	}

	if lane.connection == connection && lane.channel != nil && !lane.channel.IsClosed() {
		return nil
	}
	channel, err := connection.OpenChannel()
	if err != nil {
		if connection.IsClosed() && p.connection == connection {
			p.connection = nil
		}
		return publishError(ErrorCodeUnavailable, true, err)
	}
	if err := channel.EnableConfirms(); err != nil {
		_ = channel.Close()
		return publishError(ErrorCodeUnavailable, true, err)
	}
	lane.connection = connection
	lane.channel = channel
	lane.returns = channel.NotifyReturns(make(chan amqp.Return, 1))
	return nil
}

func declareTopology(connection brokerConnection, config Config) error {
	channel, err := connection.OpenChannel()
	if err != nil {
		return fmt.Errorf("open topology channel: %w", err)
	}
	defer channel.Close()
	if err := channel.DeclareExchange(config.Exchange); err != nil {
		return fmt.Errorf("declare durable topic exchange: %w", err)
	}
	for _, queue := range config.Queues {
		if err := channel.DeclareQuorumQueue(queue.Name); err != nil {
			return fmt.Errorf("declare quorum queue %q: %w", queue.Name, err)
		}
		for _, routingKey := range queue.BindingKeys {
			if err := channel.BindQueue(queue.Name, routingKey, config.Exchange); err != nil {
				return fmt.Errorf("bind quorum queue %q: %w", queue.Name, err)
			}
		}
	}
	return nil
}

type publishStartResult struct {
	confirmation brokerConfirmation
	err          error
}

func (p *Publisher) publishBounded(
	ctx context.Context,
	lane *publisherLane,
	routingKey string,
	message amqp.Publishing,
) (brokerConfirmation, error) {
	connection := lane.connection
	channel := lane.channel
	result := make(chan publishStartResult, 1)
	go func() {
		confirmation, err := channel.Publish(
			ctx,
			p.config.Exchange,
			routingKey,
			message,
		)
		result <- publishStartResult{confirmation: confirmation, err: err}
	}()

	select {
	case <-ctx.Done():
		p.abortConnection(connection)
		p.resetLane(lane)
		return nil, ctx.Err()
	case started := <-result:
		if started.err != nil {
			p.resetLane(lane)
			if connection != nil && connection.IsClosed() {
				p.forgetConnection(connection)
			}
			return nil, publishError(ErrorCodePublish, true, started.err)
		}
		if started.confirmation == nil {
			p.abortConnection(lane.connection)
			p.resetLane(lane)
			return nil, publishError(
				ErrorCodeConfirmMissing,
				true,
				errors.New("confirm mode returned no deferred confirmation"),
			)
		}
		return started.confirmation, nil
	}
}

func (p *Publisher) abortConnection(connection brokerConnection) {
	if connection == nil {
		return
	}
	p.forgetConnection(connection)
	go func() {
		_ = connection.CloseDeadline(time.Now().Add(p.config.CloseTimeout))
	}()
}

func (p *Publisher) forgetConnection(connection brokerConnection) {
	p.mu.Lock()
	if p.connection == connection {
		p.connection = nil
	}
	p.mu.Unlock()
}

func (p *Publisher) resetLane(lane *publisherLane) {
	if lane.channel != nil && !lane.channel.IsClosed() {
		channel := lane.channel
		go func() {
			_ = channel.Close()
		}()
	}
	lane.connection = nil
	lane.channel = nil
	lane.returns = nil
}

func (p *Publisher) makePublishing(publication ports.OutboxPublication) amqp.Publishing {
	event := publication.Event
	return amqp.Publishing{
		Headers: amqp.Table{
			"x-mail-aggregate-type":      event.AggregateType,
			"x-mail-aggregate-id":        event.AggregateID,
			"x-mail-aggregate-sequence":  int64(event.AggregateSequence),
			"x-mail-dispatch-generation": int64(event.DispatchGeneration),
			"x-mail-publish-attempt":     int64(publication.AttemptNumber),
		},
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		MessageId:     event.ID,
		CorrelationId: event.AggregateID,
		Timestamp:     p.now().UTC(),
		Type:          event.EventType,
		AppId:         p.config.ApplicationID,
		Body:          append([]byte(nil), event.Payload...),
	}
}

func validatePublication(publication ports.OutboxPublication) error {
	if err := publication.Event.Validate(); err != nil {
		return err
	}
	if publication.AttemptNumber == 0 {
		return errors.New("publish attempt number must be positive")
	}
	if publication.Event.AggregateSequence > math.MaxInt64 ||
		publication.Event.DispatchGeneration > math.MaxInt64 {
		return errors.New("event counters must fit signed AMQP long headers")
	}
	return nil
}

func publishError(code string, retryable bool, cause error) error {
	return ports.NewOutboxPublishError(code, retryable, cause)
}
