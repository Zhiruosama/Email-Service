//go:build integration

package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	notificationapp "github.com/Zhiruosama/Email-Service/internal/application/notification"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	consumerrabbit "github.com/Zhiruosama/Email-Service/internal/consumer/rabbitmq"
	mqcontract "github.com/Zhiruosama/Email-Service/internal/messaging/rabbitmq"
	"github.com/Zhiruosama/Email-Service/internal/testkit/rabbitmqcontainer"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQLifecycleConsumer(t *testing.T) {
	instance := rabbitmqcontainer.Start(t)
	applyConsumerReliabilityPolicy(t, instance)
	config := consumerrabbit.DefaultLifecycleConfig(instance.URL, "lifecycle-integration")
	config.LaneCount = 2
	config.PrefetchPerLane = 1
	config.ReconnectBase = 10 * time.Millisecond
	config.ReconnectCap = 100 * time.Millisecond
	config.TransientRequeueBase = time.Millisecond
	config.TransientRequeueCap = time.Millisecond
	config.ShutdownTimeout = 3 * time.Second
	processor := newIntegrationNotificationProcessor()
	consumer, err := consumerrabbit.NewLifecycle(config, processor)
	if err != nil {
		t.Fatalf("create lifecycle consumer: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- consumer.Run(runCtx) }()
	t.Cleanup(func() {
		cancelRun()
		select {
		case err := <-runResult:
			if err != nil {
				t.Errorf("stop lifecycle consumer: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("lifecycle consumer did not stop")
		}
	})
	waitForConsumerQueue(t, instance.URL, config.Queue, int(config.LaneCount), 10*time.Second)

	t.Run("valid lifecycle event is processed then acknowledged", func(t *testing.T) {
		eventID := "f3000000-0000-4000-8000-000000000001"
		publishLifecycleDelivery(t, instance.URL, config, eventID, true)
		observation := processor.waitFor(t, eventID, 5*time.Second)
		if observation.call != 1 {
			t.Fatalf("processor call = %d, want 1", observation.call)
		}
		waitForQueueCount(t, instance.URL, config.Queue, 0, 5*time.Second)
	})

	t.Run("transient callback failure is delayed then retried", func(t *testing.T) {
		eventID := "f4000000-0000-4000-8000-000000000001"
		processor.failOnce(eventID, ports.NewDeliveryEventSubscriberError("GRPC_UNAVAILABLE", true, nil))
		publishLifecycleDelivery(t, instance.URL, config, eventID, true)
		first := processor.waitFor(t, eventID, 5*time.Second)
		second := processor.waitFor(t, eventID, 5*time.Second)
		if first.call != 1 || second.call != 2 {
			t.Fatalf("processor calls = %d then %d, want 1 then 2", first.call, second.call)
		}
		if elapsed := second.at.Sub(first.at); elapsed < 800*time.Millisecond {
			t.Fatalf("broker retry delay = %s, want approximately one second or more", elapsed)
		}
	})

	t.Run("permanent callback failure is dead-lettered", func(t *testing.T) {
		eventID := "f5000000-0000-4000-8000-000000000001"
		processor.alwaysFail(eventID, ports.NewDeliveryEventSubscriberError("GRPC_PERMISSION_DENIED", false, nil))
		publishLifecycleDelivery(t, instance.URL, config, eventID, true)
		_ = processor.waitFor(t, eventID, 5*time.Second)
		connection, channel := openRabbitMQInspectionChannel(t, instance.URL)
		defer connection.Close()
		defer channel.Close()
		dead := getRabbitMQDeliveryWithin(t, instance, channel, config.DeadLetterQueue, 20*time.Second)
		if dead.MessageId != eventID {
			t.Fatalf("dead-letter message id = %q, want %q", dead.MessageId, eventID)
		}
		if err := dead.Ack(false); err != nil {
			t.Fatalf("ack lifecycle dead-letter: %v", err)
		}
	})

	t.Run("malformed lifecycle delivery is dead-lettered without processing", func(t *testing.T) {
		eventID := "f6000000-0000-4000-8000-000000000001"
		publishLifecycleDelivery(t, instance.URL, config, eventID, false)
		connection, channel := openRabbitMQInspectionChannel(t, instance.URL)
		defer connection.Close()
		defer channel.Close()
		dead := getRabbitMQDeliveryWithin(t, instance, channel, config.DeadLetterQueue, 20*time.Second)
		if dead.MessageId != eventID {
			t.Fatalf("dead-letter message id = %q, want %q", dead.MessageId, eventID)
		}
		if err := dead.Ack(false); err != nil {
			t.Fatalf("ack malformed lifecycle dead-letter: %v", err)
		}
		if processor.callCount(eventID) != 0 {
			t.Fatalf("malformed lifecycle delivery reached processor")
		}
	})
}

func publishLifecycleDelivery(
	t *testing.T,
	brokerURL string,
	config consumerrabbit.Config,
	eventID string,
	valid bool,
) {
	t.Helper()
	connection, channel := openRabbitMQInspectionChannel(t, brokerURL)
	defer connection.Close()
	defer channel.Close()
	contentType := mqcontract.ContentTypeJSON
	if !valid {
		contentType = "text/plain"
	}
	err := channel.PublishWithContext(
		context.Background(),
		config.Exchange,
		mqcontract.RoutingStatusChanged,
		true,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				mqcontract.HeaderAggregateType:      "MAIL_MESSAGE",
				mqcontract.HeaderAggregateID:        "f7000000-0000-4000-8000-000000000001",
				mqcontract.HeaderAggregateSequence:  int64(2),
				mqcontract.HeaderDispatchGeneration: int64(1),
				mqcontract.HeaderPublishAttempt:     int64(1),
			},
			ContentType:   contentType,
			DeliveryMode:  amqp.Persistent,
			MessageId:     eventID,
			CorrelationId: "f7000000-0000-4000-8000-000000000001",
			Type:          mqcontract.EventStatusChanged,
			AppId:         config.ApplicationID,
			Body: []byte(`{
				"schema_version":1,
				"tenant_id":"f8000000-0000-4000-8000-000000000001",
				"message_id":"f7000000-0000-4000-8000-000000000001",
				"event_type":"MESSAGE_STATUS_CHANGED",
				"from":"ACCEPTED",
				"to":"QUEUED",
				"occurred_at":"2026-08-09T16:00:00Z",
				"sequence":2,
				"dispatch_generation":1,
				"attempt_number":0
			}`),
		},
	)
	if err != nil {
		t.Fatalf("publish lifecycle delivery: %v", err)
	}
}

type integrationNotificationObservation struct {
	eventID string
	call    int
	at      time.Time
}

type integrationNotificationProcessor struct {
	mu       sync.Mutex
	calls    map[string]int
	failures map[string][]error
	observed chan integrationNotificationObservation
}

func newIntegrationNotificationProcessor() *integrationNotificationProcessor {
	return &integrationNotificationProcessor{
		calls:    make(map[string]int),
		failures: make(map[string][]error),
		observed: make(chan integrationNotificationObservation, 16),
	}
}

func (p *integrationNotificationProcessor) failOnce(eventID string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures[eventID] = []error{err}
}

func (p *integrationNotificationProcessor) alwaysFail(eventID string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures[eventID] = []error{err, err, err}
}

func (p *integrationNotificationProcessor) Process(
	_ context.Context,
	command notificationapp.Command,
) (notificationapp.Result, error) {
	p.mu.Lock()
	p.calls[command.EventID]++
	call := p.calls[command.EventID]
	var err error
	if failures := p.failures[command.EventID]; len(failures) > 0 {
		err = failures[0]
		p.failures[command.EventID] = failures[1:]
	}
	p.mu.Unlock()
	p.observed <- integrationNotificationObservation{eventID: command.EventID, call: call, at: time.Now()}
	return notificationapp.Result{}, err
}

func (p *integrationNotificationProcessor) waitFor(
	t *testing.T,
	eventID string,
	timeout time.Duration,
) integrationNotificationObservation {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case observation := <-p.observed:
			if observation.eventID == eventID {
				return observation
			}
		case <-timer.C:
			t.Fatalf("notification processor did not observe event %q", eventID)
		}
	}
}

func (p *integrationNotificationProcessor) callCount(eventID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[eventID]
}
