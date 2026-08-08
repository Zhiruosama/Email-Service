package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublisherPublishesConfirmedPersistentMessage(t *testing.T) {
	behavior := &fakeBrokerBehavior{confirmation: fixedConfirmation{ack: true}}
	factory := &fakeConnectionFactory{behaviors: []*fakeBrokerBehavior{behavior}}
	config := publisherTestConfig()
	config.ChannelPoolSize = 1
	publisher, err := newPublisher(config, factory.Dial)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	publisher.now = func() time.Time {
		return time.Date(2026, 8, 9, 18, 0, 0, 0, time.UTC)
	}

	publication := validPublication()
	if err := publisher.Publish(context.Background(), publication); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if factory.DialCount() != 1 {
		t.Fatalf("dial count = %d, want 1", factory.DialCount())
	}
	connection := factory.Connection(0)
	assertDeclaredTopology(t, connection, config)
	published := behavior.Publications()
	if len(published) != 1 {
		t.Fatalf("published count = %d, want 1", len(published))
	}
	message := published[0]
	if message.exchange != config.Exchange || message.routingKey != RoutingKeyDispatchRequested {
		t.Fatalf("destination = %q/%q", message.exchange, message.routingKey)
	}
	if message.message.DeliveryMode != amqp.Persistent || message.message.ContentType != "application/json" {
		t.Fatalf("delivery metadata = %#v", message.message)
	}
	if message.message.MessageId != publication.Event.ID ||
		message.message.CorrelationId != publication.Event.AggregateID ||
		message.message.Type != publication.Event.EventType ||
		message.message.AppId != config.ApplicationID {
		t.Fatalf("message properties = %#v", message.message)
	}
	if got := message.message.Headers["x-mail-aggregate-sequence"]; got != int64(7) {
		t.Fatalf("aggregate sequence header = %#v", got)
	}
	if got := message.message.Headers["x-mail-dispatch-generation"]; got != int64(3) {
		t.Fatalf("dispatch generation header = %#v", got)
	}
	if got := message.message.Headers["x-mail-publish-attempt"]; got != int64(2) {
		t.Fatalf("publish attempt header = %#v", got)
	}
	publication.Event.Payload[0] = '['
	if string(message.message.Body) != `{"schema_version":1}` {
		t.Fatalf("published body aliased caller payload: %s", message.message.Body)
	}

	second := validPublication()
	second.Event.ID = "30000000-0000-4000-8000-000000000002"
	if err := publisher.Publish(context.Background(), second); err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if factory.DialCount() != 1 || connection.OpenCount() != 2 {
		t.Fatalf(
			"connection reuse: dials=%d channels=%d, want 1/2",
			factory.DialCount(),
			connection.OpenCount(),
		)
	}
}

func TestPublisherDoesNotDialForInvalidOrUnsupportedEvent(t *testing.T) {
	factory := &fakeConnectionFactory{}
	publisher, err := newPublisher(publisherTestConfig(), factory.Dial)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}

	invalid := validPublication()
	invalid.AttemptNumber = 0
	assertPublishFailure(t, publisher.Publish(context.Background(), invalid), ErrorCodeInvalidPublication, false)

	unsupported := validPublication()
	unsupported.Event.EventType = "UNKNOWN_EVENT"
	assertPublishFailure(t, publisher.Publish(context.Background(), unsupported), ErrorCodeUnsupportedEvent, false)
	if factory.DialCount() != 0 {
		t.Fatalf("dial count = %d, want 0", factory.DialCount())
	}
}

func TestPublisherClassifiesBrokerFailures(t *testing.T) {
	tests := []struct {
		name      string
		behavior  *fakeBrokerBehavior
		code      string
		retryable bool
	}{
		{
			name:      "publish",
			behavior:  &fakeBrokerBehavior{publishErr: errors.New("socket closed")},
			code:      ErrorCodePublish,
			retryable: true,
		},
		{
			name:      "missing confirm",
			behavior:  &fakeBrokerBehavior{},
			code:      ErrorCodeConfirmMissing,
			retryable: true,
		},
		{
			name:      "nack",
			behavior:  &fakeBrokerBehavior{confirmation: fixedConfirmation{ack: false}},
			code:      ErrorCodeNack,
			retryable: true,
		},
		{
			name: "unroutable",
			behavior: &fakeBrokerBehavior{
				confirmation: fixedConfirmation{ack: true},
				returnMode:   returnMatching,
			},
			code:      ErrorCodeUnroutable,
			retryable: false,
		},
		{
			name: "return mismatch",
			behavior: &fakeBrokerBehavior{
				confirmation: fixedConfirmation{ack: true},
				returnMode:   returnMismatched,
			},
			code:      ErrorCodeProtocol,
			retryable: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			factory := &fakeConnectionFactory{behaviors: []*fakeBrokerBehavior{test.behavior}}
			publisher, err := newPublisher(publisherTestConfig(), factory.Dial)
			if err != nil {
				t.Fatalf("create publisher: %v", err)
			}
			assertPublishFailure(
				t,
				publisher.Publish(context.Background(), validPublication()),
				test.code,
				test.retryable,
			)
		})
	}
}

func TestPublisherClassifiesConnectionAndTopologyFailures(t *testing.T) {
	t.Run("dial", func(t *testing.T) {
		factory := &fakeConnectionFactory{dialErrors: []error{errors.New("connection refused")}}
		publisher, err := newPublisher(publisherTestConfig(), factory.Dial)
		if err != nil {
			t.Fatalf("create publisher: %v", err)
		}
		assertPublishFailure(
			t,
			publisher.Publish(context.Background(), validPublication()),
			ErrorCodeUnavailable,
			true,
		)
	})

	t.Run("topology", func(t *testing.T) {
		behavior := &fakeBrokerBehavior{topologyErr: errors.New("precondition failed")}
		factory := &fakeConnectionFactory{behaviors: []*fakeBrokerBehavior{behavior}}
		publisher, err := newPublisher(publisherTestConfig(), factory.Dial)
		if err != nil {
			t.Fatalf("create publisher: %v", err)
		}
		assertPublishFailure(
			t,
			publisher.Publish(context.Background(), validPublication()),
			ErrorCodeTopology,
			true,
		)
		if !factory.Connection(0).IsClosed() {
			t.Fatal("connection with failed topology was not closed")
		}
	})
}

func TestPublisherConfirmTimeoutAbortsConnectionAndRedials(t *testing.T) {
	timedOut := &fakeBrokerBehavior{confirmation: contextConfirmation{}}
	recovered := &fakeBrokerBehavior{confirmation: fixedConfirmation{ack: true}}
	factory := &fakeConnectionFactory{behaviors: []*fakeBrokerBehavior{timedOut, recovered}}
	config := publisherTestConfig()
	config.ChannelPoolSize = 1
	publisher, err := newPublisher(config, factory.Dial)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := publisher.Publish(ctx, validPublication()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error = %v, want context deadline", err)
	}
	waitFor(t, time.Second, func() bool { return factory.Connection(0).IsClosed() })

	if err := publisher.Publish(context.Background(), validPublication()); err != nil {
		t.Fatalf("publish after timeout: %v", err)
	}
	if factory.DialCount() != 2 {
		t.Fatalf("dial count = %d, want 2", factory.DialCount())
	}
}

func TestPublisherUsesExclusiveChannelPerConcurrentPublish(t *testing.T) {
	gate := &gateConfirmation{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	behavior := &fakeBrokerBehavior{confirmation: gate}
	factory := &fakeConnectionFactory{behaviors: []*fakeBrokerBehavior{behavior}}
	config := publisherTestConfig()
	config.ChannelPoolSize = 2
	publisher, err := newPublisher(config, factory.Dial)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}

	errorsChannel := make(chan error, 2)
	for index := range 2 {
		publication := validPublication()
		if index == 1 {
			publication.Event.ID = "30000000-0000-4000-8000-000000000002"
		}
		go func() {
			errorsChannel <- publisher.Publish(context.Background(), publication)
		}()
	}
	for range 2 {
		select {
		case <-gate.started:
		case <-time.After(time.Second):
			t.Fatal("concurrent publication did not reach confirm wait")
		}
	}
	if opened := factory.Connection(0).OpenCount(); opened != 3 {
		t.Fatalf("opened channels = %d, want topology + two publisher channels", opened)
	}
	close(gate.release)
	for range 2 {
		if err := <-errorsChannel; err != nil {
			t.Fatalf("concurrent publish: %v", err)
		}
	}
}

func TestPublisherBoundsBlockedPublishStart(t *testing.T) {
	behavior := &fakeBrokerBehavior{
		confirmation: fixedConfirmation{ack: true},
		blockPublish: true,
	}
	factory := &fakeConnectionFactory{behaviors: []*fakeBrokerBehavior{behavior}}
	publisher, err := newPublisher(publisherTestConfig(), factory.Dial)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := publisher.Publish(ctx, validPublication()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked publish error = %v, want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("blocked publish returned after %s", elapsed)
	}
	waitFor(t, time.Second, func() bool { return factory.Connection(0).IsClosed() })
}

func TestPublisherCloseIsIdempotentAndRejectsFurtherPublishing(t *testing.T) {
	behavior := &fakeBrokerBehavior{confirmation: fixedConfirmation{ack: true}}
	factory := &fakeConnectionFactory{behaviors: []*fakeBrokerBehavior{behavior}}
	publisher, err := newPublisher(publisherTestConfig(), factory.Dial)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	if err := publisher.Publish(context.Background(), validPublication()); err != nil {
		t.Fatalf("initial publish: %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	assertPublishFailure(
		t,
		publisher.Publish(context.Background(), validPublication()),
		ErrorCodeClosed,
		true,
	)
}

func TestNewPublisherRejectsNilFactory(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("newPublisher with nil factory did not panic")
		}
	}()
	_, _ = newPublisher(publisherTestConfig(), nil)
}

func publisherTestConfig() Config {
	config := DefaultConfig("amqp://guest:guest@localhost:5672/", "test-relay")
	config.ChannelPoolSize = 2
	config.CloseTimeout = 100 * time.Millisecond
	return config
}

func validPublication() ports.OutboxPublication {
	return ports.OutboxPublication{
		Event: ports.OutboxEvent{
			ID:                 "30000000-0000-4000-8000-000000000001",
			AggregateType:      ports.OutboxAggregateMailMessage,
			AggregateID:        "40000000-0000-4000-8000-000000000001",
			EventType:          "MESSAGE_DISPATCH_REQUESTED",
			AggregateSequence:  7,
			DispatchGeneration: 3,
			Payload:            []byte(`{"schema_version":1}`),
		},
		AttemptNumber: 2,
	}
}

func assertPublishFailure(t *testing.T, err error, code string, retryable bool) {
	t.Helper()
	var failure *ports.OutboxPublishError
	if !errors.As(err, &failure) {
		t.Fatalf("error type = %T, want *OutboxPublishError: %v", err, err)
	}
	if failure.Code != code || failure.Retryable != retryable {
		t.Fatalf(
			"failure = code %q retryable %t, want %q/%t",
			failure.Code,
			failure.Retryable,
			code,
			retryable,
		)
	}
	if failure.Cause() == nil {
		t.Fatal("publish failure lost internal cause")
	}
}

func assertDeclaredTopology(t *testing.T, connection *fakeBrokerConnection, config Config) {
	t.Helper()
	channels := connection.Channels()
	if len(channels) < 1 {
		t.Fatal("topology channel was not opened")
	}
	topology := channels[0]
	if topology.exchange != config.Exchange {
		t.Fatalf("declared exchange = %q, want %q", topology.exchange, config.Exchange)
	}
	if len(topology.queues) != len(config.Queues) {
		t.Fatalf("declared queue count = %d, want %d", len(topology.queues), len(config.Queues))
	}
	wantBindings := 0
	for _, queue := range config.Queues {
		wantBindings += len(queue.BindingKeys)
	}
	if len(topology.bindings) != wantBindings {
		t.Fatalf("binding count = %d, want %d", len(topology.bindings), wantBindings)
	}
	if !topology.IsClosed() {
		t.Fatal("topology channel was not closed")
	}
}

type returnMode uint8

const (
	returnNone returnMode = iota
	returnMatching
	returnMismatched
)

type recordedPublish struct {
	exchange   string
	routingKey string
	message    amqp.Publishing
}

type fakeBrokerBehavior struct {
	mu             sync.Mutex
	topologyErr    error
	publishErr     error
	confirmation   brokerConfirmation
	returnMode     returnMode
	blockPublish   bool
	closeOnce      sync.Once
	connectionDone chan struct{}
	publications   []recordedPublish
}

func (b *fakeBrokerBehavior) initialize() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.connectionDone == nil {
		b.connectionDone = make(chan struct{})
	}
}

func (b *fakeBrokerBehavior) Publications() []recordedPublish {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make([]recordedPublish, len(b.publications))
	copy(result, b.publications)
	return result
}

func (b *fakeBrokerBehavior) closeConnection() {
	b.initialize()
	b.closeOnce.Do(func() { close(b.connectionDone) })
}

type fakeConnectionFactory struct {
	mu          sync.Mutex
	behaviors   []*fakeBrokerBehavior
	dialErrors  []error
	dialCount   int
	connections []*fakeBrokerConnection
}

func (f *fakeConnectionFactory) Dial(context.Context, Config) (brokerConnection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := f.dialCount
	f.dialCount++
	if index < len(f.dialErrors) && f.dialErrors[index] != nil {
		return nil, f.dialErrors[index]
	}
	behavior := &fakeBrokerBehavior{confirmation: fixedConfirmation{ack: true}}
	if index < len(f.behaviors) && f.behaviors[index] != nil {
		behavior = f.behaviors[index]
	}
	behavior.initialize()
	connection := &fakeBrokerConnection{behavior: behavior}
	f.connections = append(f.connections, connection)
	return connection, nil
}

func (f *fakeConnectionFactory) DialCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dialCount
}

func (f *fakeConnectionFactory) Connection(index int) *fakeBrokerConnection {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connections[index]
}

type fakeBrokerConnection struct {
	mu       sync.Mutex
	behavior *fakeBrokerBehavior
	channels []*fakeBrokerChannel
	closed   bool
}

func (c *fakeBrokerConnection) OpenChannel() (brokerChannel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("connection closed")
	}
	channel := &fakeBrokerChannel{behavior: c.behavior}
	c.channels = append(c.channels, channel)
	return channel, nil
}

func (c *fakeBrokerConnection) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeBrokerConnection) CloseDeadline(time.Time) error {
	c.mu.Lock()
	c.closed = true
	channels := append([]*fakeBrokerChannel(nil), c.channels...)
	c.mu.Unlock()
	c.behavior.closeConnection()
	for _, channel := range channels {
		_ = channel.Close()
	}
	return nil
}

func (c *fakeBrokerConnection) Channels() []*fakeBrokerChannel {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*fakeBrokerChannel(nil), c.channels...)
}

func (c *fakeBrokerConnection) OpenCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.channels)
}

type fakeBinding struct {
	queue    string
	key      string
	exchange string
}

type fakeBrokerChannel struct {
	mu         sync.Mutex
	behavior   *fakeBrokerBehavior
	closed     bool
	exchange   string
	queues     []string
	bindings   []fakeBinding
	confirming bool
	returns    chan amqp.Return
}

func (c *fakeBrokerChannel) DeclareExchange(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.behavior.topologyErr != nil {
		return c.behavior.topologyErr
	}
	c.exchange = name
	return nil
}

func (c *fakeBrokerChannel) DeclareQuorumQueue(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queues = append(c.queues, name)
	return nil
}

func (c *fakeBrokerChannel) BindQueue(queue, key, exchange string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bindings = append(c.bindings, fakeBinding{queue: queue, key: key, exchange: exchange})
	return nil
}

func (c *fakeBrokerChannel) EnableConfirms() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.confirming = true
	return nil
}

func (c *fakeBrokerChannel) NotifyReturns(receiver chan amqp.Return) <-chan amqp.Return {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.returns = receiver
	return receiver
}

func (c *fakeBrokerChannel) Publish(
	ctx context.Context,
	exchange string,
	routingKey string,
	message amqp.Publishing,
) (brokerConfirmation, error) {
	c.behavior.mu.Lock()
	c.behavior.publications = append(c.behavior.publications, recordedPublish{
		exchange:   exchange,
		routingKey: routingKey,
		message:    cloneAMQPMessage(message),
	})
	block := c.behavior.blockPublish
	publishErr := c.behavior.publishErr
	confirmation := c.behavior.confirmation
	mode := c.behavior.returnMode
	done := c.behavior.connectionDone
	c.behavior.mu.Unlock()

	if block {
		<-done
		return nil, errors.New("connection closed during blocked publish")
	}
	if publishErr != nil {
		return nil, publishErr
	}
	c.mu.Lock()
	returns := c.returns
	c.mu.Unlock()
	if mode != returnNone {
		messageID := message.MessageId
		if mode == returnMismatched {
			messageID = "30000000-0000-4000-8000-000000000099"
		}
		returns <- amqp.Return{
			ReplyCode:  312,
			ReplyText:  "NO_ROUTE",
			Exchange:   exchange,
			RoutingKey: routingKey,
			MessageId:  messageID,
		}
	}
	return confirmation, nil
}

func (c *fakeBrokerChannel) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeBrokerChannel) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.returns != nil {
		close(c.returns)
	}
	return nil
}

type fixedConfirmation struct {
	ack bool
	err error
}

func (c fixedConfirmation) WaitContext(context.Context) (bool, error) {
	return c.ack, c.err
}

type contextConfirmation struct{}

func (contextConfirmation) WaitContext(ctx context.Context) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}

type gateConfirmation struct {
	started chan struct{}
	release chan struct{}
}

func (c *gateConfirmation) WaitContext(ctx context.Context) (bool, error) {
	c.started <- struct{}{}
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-c.release:
		return true, nil
	}
}

func cloneAMQPMessage(message amqp.Publishing) amqp.Publishing {
	cloned := message
	cloned.Body = append([]byte(nil), message.Body...)
	cloned.Headers = make(amqp.Table, len(message.Headers))
	for key, value := range message.Headers {
		cloned.Headers[key] = value
	}
	return cloned
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}
