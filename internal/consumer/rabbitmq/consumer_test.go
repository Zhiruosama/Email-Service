package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	deliveryapp "github.com/Zhiruosama/Email-Service/internal/application/delivery"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestHandleDeliveryMapsResultToAcknowledgement(t *testing.T) {
	t.Parallel()
	transientErr := errors.New("database temporarily unavailable")
	tests := []struct {
		name        string
		mutate      func(*amqp.Delivery)
		processErr  error
		wantAck     int
		wantNack    int
		wantReject  int
		wantRequeue bool
	}{
		{name: "success ACKs", wantAck: 1},
		{
			name: "malformed delivery is rejected without requeue",
			mutate: func(delivery *amqp.Delivery) {
				delivery.ContentType = "text/plain"
			},
			wantNack: 1,
		},
		{
			name:       "poison worker error is rejected without requeue",
			processErr: deliveryapp.ErrDispatchInvariant,
			wantNack:   1,
		},
		{
			name:        "transient worker error is rejected and requeued",
			processErr:  transientErr,
			wantReject:  1,
			wantRequeue: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultConfig("amqp://guest:guest@localhost:5672/", "ack-test")
			config.TransientRequeueBase = time.Millisecond
			config.TransientRequeueCap = time.Millisecond
			processor := &recordingProcessor{err: test.processErr}
			consumer, err := newConsumer(config, processor, unusedConnectionFactory)
			if err != nil {
				t.Fatalf("new consumer: %v", err)
			}
			acknowledger := &recordingAcknowledger{}
			delivery := validDispatchDelivery(config)
			delivery.Acknowledger = acknowledger
			delivery.DeliveryTag = 41
			if test.mutate != nil {
				test.mutate(&delivery)
			}

			if err := consumer.handleDelivery(context.Background(), delivery); err != nil {
				t.Fatalf("handle delivery: %v", err)
			}
			if acknowledger.ack != test.wantAck ||
				acknowledger.nack != test.wantNack ||
				acknowledger.reject != test.wantReject ||
				acknowledger.requeue != test.wantRequeue {
				t.Fatalf(
					"acknowledgement = ack:%d nack:%d reject:%d requeue:%t",
					acknowledger.ack,
					acknowledger.nack,
					acknowledger.reject,
					acknowledger.requeue,
				)
			}
			wantCalls := 1
			if test.mutate != nil {
				wantCalls = 0
			}
			if processor.calls != wantCalls {
				t.Fatalf("processor calls = %d, want %d", processor.calls, wantCalls)
			}
		})
	}
}

func TestHandleDeliveryReturnsAckFailure(t *testing.T) {
	t.Parallel()
	config := DefaultConfig("amqp://guest:guest@localhost:5672/", "ack-error")
	consumer, err := newConsumer(config, &recordingProcessor{}, unusedConnectionFactory)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	delivery := validDispatchDelivery(config)
	delivery.DeliveryTag = 7
	delivery.Acknowledger = &recordingAcknowledger{err: errors.New("channel closed")}

	if err := consumer.handleDelivery(context.Background(), delivery); !errors.Is(err, ErrConsumerAck) {
		t.Fatalf("handle delivery error = %v, want ErrConsumerAck", err)
	}
}

func TestConsumeLaneUsesManualAckAndBoundedPrefetch(t *testing.T) {
	t.Parallel()
	config := DefaultConfig("amqp://guest:guest@localhost:5672/", "lane-test")
	config.PrefetchPerLane = 3
	processor := &recordingProcessor{}
	consumer, err := newConsumer(config, processor, unusedConnectionFactory)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	deliveries := make(chan amqp.Delivery, 1)
	acknowledger := &recordingAcknowledger{}
	delivery := validDispatchDelivery(config)
	delivery.Acknowledger = acknowledger
	delivery.DeliveryTag = 9
	deliveries <- delivery
	close(deliveries)
	channel := &recordingChannel{deliveries: deliveries}

	err = consumer.consumeLane(context.Background(), channel, 2)
	if !errors.Is(err, ErrConsumerChannel) {
		t.Fatalf("consume lane error = %v, want closed stream channel error", err)
	}
	if channel.prefetch != 3 {
		t.Fatalf("prefetch = %d, want 3", channel.prefetch)
	}
	if channel.autoAck {
		t.Fatal("consume enabled auto-ack")
	}
	if channel.consumerTag != "mail-worker-lane-test-002" {
		t.Fatalf("consumer tag = %q", channel.consumerTag)
	}
	if processor.calls != 1 || acknowledger.ack != 1 {
		t.Fatalf("processed/ACK count = %d/%d, want 1/1", processor.calls, acknowledger.ack)
	}
}

func TestTransientDeliveryDuringShutdownRemainsUnacked(t *testing.T) {
	t.Parallel()
	config := DefaultConfig("amqp://guest:guest@localhost:5672/", "shutdown-test")
	config.TransientRequeueBase = time.Second
	config.TransientRequeueCap = time.Second
	consumer, err := newConsumer(
		config,
		&recordingProcessor{err: ports.ErrMessageRepository},
		unusedConnectionFactory,
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	acknowledger := &recordingAcknowledger{}
	delivery := validDispatchDelivery(config)
	delivery.Acknowledger = acknowledger
	delivery.DeliveryTag = 11
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := consumer.handleDelivery(ctx, delivery); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle delivery error = %v, want context cancellation", err)
	}
	if acknowledger.ack+acknowledger.nack+acknowledger.reject != 0 {
		t.Fatal("delivery was acknowledged during shutdown")
	}
}

func TestRunSessionCreatesIndependentLaneChannelsAndStopsGracefully(t *testing.T) {
	t.Parallel()
	config := DefaultConfig("amqp://guest:guest@localhost:5672/", "session-test")
	config.LaneCount = 3
	config.PrefetchPerLane = 2
	config.ShutdownTimeout = time.Second
	consumer, err := newConsumer(config, &recordingProcessor{}, unusedConnectionFactory)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	topologyChannel := &recordingChannel{}
	laneChannels := []*recordingChannel{
		{closeDeliveriesWithContext: true},
		{closeDeliveriesWithContext: true},
		{closeDeliveriesWithContext: true},
	}
	connection := &scriptedConnection{
		channels: []brokerChannel{
			topologyChannel,
			laneChannels[0],
			laneChannels[1],
			laneChannels[2],
		},
		notifyClose: make(chan *amqp.Error),
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- consumer.runSession(ctx, connection) }()

	deadline := time.Now().Add(time.Second)
	for connection.openCount() != 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if connection.openCount() != 4 {
		cancel()
		t.Fatalf("opened channels = %d, want topology + 3 lanes", connection.openCount())
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("graceful session shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not stop within shutdown bound")
	}

	for index, channel := range laneChannels {
		if channel.prefetch != 2 {
			t.Fatalf("lane %d prefetch = %d, want 2", index+1, channel.prefetch)
		}
		if channel.autoAck {
			t.Fatalf("lane %d enabled auto-ack", index+1)
		}
	}
	if !connection.wasClosed() {
		t.Fatal("AMQP connection was not closed after all lanes stopped")
	}
}

type recordingProcessor struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (p *recordingProcessor) Process(
	_ context.Context,
	_ deliveryapp.DispatchCommand,
) (deliveryapp.DispatchResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return deliveryapp.DispatchResult{}, p.err
}

type recordingAcknowledger struct {
	ack     int
	nack    int
	reject  int
	requeue bool
	err     error
}

func (a *recordingAcknowledger) Ack(_ uint64, _ bool) error {
	a.ack++
	return a.err
}

func (a *recordingAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	a.nack++
	a.requeue = requeue
	return a.err
}

func (a *recordingAcknowledger) Reject(_ uint64, requeue bool) error {
	a.reject++
	a.requeue = requeue
	return a.err
}

type recordingChannel struct {
	deliveries                 <-chan amqp.Delivery
	prefetch                   int
	autoAck                    bool
	consumerTag                string
	closeDeliveriesWithContext bool
}

func (c *recordingChannel) ExchangeDeclare(string, string, bool, bool, bool, bool, amqp.Table) error {
	return nil
}

func (c *recordingChannel) QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{}, nil
}

func (c *recordingChannel) QueueBind(string, string, string, bool, amqp.Table) error {
	return nil
}

func (c *recordingChannel) Qos(prefetchCount, _ int, _ bool) error {
	c.prefetch = prefetchCount
	return nil
}

func (c *recordingChannel) ConsumeWithContext(
	ctx context.Context,
	_ string,
	consumerTag string,
	autoAck bool,
	_ bool,
	_ bool,
	_ bool,
	_ amqp.Table,
) (<-chan amqp.Delivery, error) {
	c.consumerTag = consumerTag
	c.autoAck = autoAck
	if c.closeDeliveriesWithContext {
		deliveries := make(chan amqp.Delivery)
		go func() {
			<-ctx.Done()
			close(deliveries)
		}()
		return deliveries, nil
	}
	return c.deliveries, nil
}

func (*recordingChannel) IsClosed() bool { return false }
func (*recordingChannel) Close() error   { return nil }

func unusedConnectionFactory(context.Context, Config) (brokerConnection, error) {
	return nil, errors.New("unexpected connection attempt")
}

type scriptedConnection struct {
	mu          sync.Mutex
	channels    []brokerChannel
	opened      int
	closed      bool
	notifyClose chan *amqp.Error
}

func (c *scriptedConnection) OpenChannel() (brokerChannel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.opened >= len(c.channels) {
		return nil, errors.New("no scripted channel")
	}
	channel := c.channels[c.opened]
	c.opened++
	return channel, nil
}

func (c *scriptedConnection) NotifyClose(_ chan *amqp.Error) <-chan *amqp.Error {
	return c.notifyClose
}

func (c *scriptedConnection) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *scriptedConnection) CloseDeadline(time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *scriptedConnection) openCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opened
}

func (c *scriptedConnection) wasClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}
