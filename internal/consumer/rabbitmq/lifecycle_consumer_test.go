package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	notificationapp "github.com/Zhiruosama/Email-Service/internal/application/notification"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	mqcontract "github.com/Zhiruosama/Email-Service/internal/messaging/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestLifecycleHandleDeliveryMapsResultToAcknowledgement(t *testing.T) {
	t.Parallel()
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
			name: "malformed lifecycle delivery is dead-lettered",
			mutate: func(delivery *amqp.Delivery) {
				delivery.ContentType = "text/plain"
			},
			wantNack: 1,
		},
		{
			name:       "permanent callback failure is dead-lettered",
			processErr: ports.NewDeliveryEventSubscriberError("GRPC_PERMISSION_DENIED", false, nil),
			wantNack:   1,
		},
		{
			name:        "transient callback failure is rejected and requeued",
			processErr:  ports.NewDeliveryEventSubscriberError("GRPC_UNAVAILABLE", true, nil),
			wantReject:  1,
			wantRequeue: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultLifecycleConfig("amqp://guest:guest@localhost:5672/", "ack-test")
			config.TransientRequeueBase = time.Millisecond
			config.TransientRequeueCap = time.Millisecond
			processor := &recordingNotificationProcessor{err: test.processErr}
			consumer, err := newLifecycleConsumer(config, processor, unusedConnectionFactory)
			if err != nil {
				t.Fatalf("new lifecycle consumer: %v", err)
			}
			acknowledger := &recordingAcknowledger{}
			delivery := validLifecycleDelivery(config, mqcontract.EventStatusChanged)
			delivery.Acknowledger = acknowledger
			delivery.DeliveryTag = 51
			if test.mutate != nil {
				test.mutate(&delivery)
			}

			if err := consumer.handleLifecycleDelivery(context.Background(), delivery); err != nil {
				t.Fatalf("handle lifecycle delivery: %v", err)
			}
			if acknowledger.ack != test.wantAck || acknowledger.nack != test.wantNack ||
				acknowledger.reject != test.wantReject || acknowledger.requeue != test.wantRequeue {
				t.Fatalf("acknowledgement = ack:%d nack:%d reject:%d requeue:%t", acknowledger.ack, acknowledger.nack, acknowledger.reject, acknowledger.requeue)
			}
			wantCalls := 1
			if test.mutate != nil {
				wantCalls = 0
			}
			if processor.callCount() != wantCalls {
				t.Fatalf("processor calls = %d, want %d", processor.callCount(), wantCalls)
			}
		})
	}
}

func TestLifecycleTransientDeliveryDuringShutdownRemainsUnacked(t *testing.T) {
	t.Parallel()
	config := DefaultLifecycleConfig("amqp://guest:guest@localhost:5672/", "shutdown-test")
	config.TransientRequeueBase = time.Second
	config.TransientRequeueCap = time.Second
	consumer, err := newLifecycleConsumer(
		config,
		&recordingNotificationProcessor{err: errors.New("database unavailable")},
		unusedConnectionFactory,
	)
	if err != nil {
		t.Fatalf("new lifecycle consumer: %v", err)
	}
	acknowledger := &recordingAcknowledger{}
	delivery := validLifecycleDelivery(config, mqcontract.EventMessageAccepted)
	delivery.Acknowledger = acknowledger
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := consumer.handleLifecycleDelivery(ctx, delivery); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle lifecycle delivery error = %v, want context cancellation", err)
	}
	if acknowledger.ack+acknowledger.nack+acknowledger.reject != 0 {
		t.Fatal("lifecycle delivery was acknowledged during shutdown")
	}
}

func TestLifecycleConsumerDeclaresBothBindings(t *testing.T) {
	t.Parallel()
	config := DefaultLifecycleConfig("amqp://guest:guest@localhost:5672/", "topology-test")
	channel := &recordingChannel{}
	connection := &scriptedConnection{channels: []brokerChannel{channel}}
	if err := declareConsumerTopology(connection, config); err != nil {
		t.Fatalf("declare lifecycle topology: %v", err)
	}
	if len(channel.bindings) != 3 {
		t.Fatalf("bindings = %#v, want DLQ + two lifecycle routes", channel.bindings)
	}
}

func TestNewLifecycleConsumerRejectsNilProcessor(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("constructor did not panic")
		}
	}()
	_, _ = newLifecycleConsumer(
		DefaultLifecycleConfig("amqp://guest:guest@localhost:5672/", "nil-test"),
		nil,
		unusedConnectionFactory,
	)
}

type recordingNotificationProcessor struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (p *recordingNotificationProcessor) Process(
	_ context.Context,
	_ notificationapp.Command,
) (notificationapp.Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return notificationapp.Result{}, p.err
}

func (p *recordingNotificationProcessor) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}
