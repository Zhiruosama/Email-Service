//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	deliveryapp "github.com/Zhiruosama/Email-Service/internal/application/delivery"
	consumerrabbit "github.com/Zhiruosama/Email-Service/internal/consumer/rabbitmq"
	mqcontract "github.com/Zhiruosama/Email-Service/internal/messaging/rabbitmq"
	"github.com/Zhiruosama/Email-Service/internal/testkit/rabbitmqcontainer"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQConsumer(t *testing.T) {
	instance := rabbitmqcontainer.Start(t)
	applyConsumerReliabilityPolicy(t, instance)

	config := consumerrabbit.DefaultConfig(instance.URL, "consumer-integration")
	config.LaneCount = 2
	config.PrefetchPerLane = 1
	config.ReconnectBase = 10 * time.Millisecond
	config.ReconnectCap = 100 * time.Millisecond
	config.TransientRequeueBase = time.Millisecond
	config.TransientRequeueCap = time.Millisecond
	config.ShutdownTimeout = 3 * time.Second
	processor := newIntegrationDispatchProcessor()
	consumer, err := consumerrabbit.New(config, processor)
	if err != nil {
		t.Fatalf("create RabbitMQ consumer: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- consumer.Run(runCtx) }()
	t.Cleanup(func() {
		cancelRun()
		select {
		case err := <-runResult:
			if err != nil {
				t.Errorf("stop RabbitMQ consumer: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("RabbitMQ consumer did not stop")
		}
	})

	waitForConsumerQueue(t, instance.URL, config.Queue, int(config.LaneCount), 10*time.Second)

	t.Run("valid delivery is processed then acknowledged", func(t *testing.T) {
		eventID := "c1000000-0000-4000-8000-000000000001"
		publishDispatchDelivery(t, instance.URL, config, eventID, true)
		observation := processor.waitFor(t, eventID, 5*time.Second)
		if observation.call != 1 {
			t.Fatalf("processor call = %d, want 1", observation.call)
		}
		waitForQueueCount(t, instance.URL, config.Queue, 0, 5*time.Second)
	})

	t.Run("transient failure is delayed then retried", func(t *testing.T) {
		eventID := "c2000000-0000-4000-8000-000000000001"
		processor.failOnce(eventID)
		publishDispatchDelivery(t, instance.URL, config, eventID, true)
		first := processor.waitFor(t, eventID, 5*time.Second)
		second := processor.waitFor(t, eventID, 5*time.Second)
		if first.call != 1 || second.call != 2 {
			t.Fatalf("processor calls = %d then %d, want 1 then 2", first.call, second.call)
		}
		if elapsed := second.at.Sub(first.at); elapsed < 800*time.Millisecond {
			t.Fatalf("broker retry delay = %s, want approximately one second or more", elapsed)
		}
	})

	t.Run("malformed delivery is dead-lettered", func(t *testing.T) {
		eventID := "c3000000-0000-4000-8000-000000000001"
		publishDispatchDelivery(t, instance.URL, config, eventID, false)
		connection, channel := openRabbitMQInspectionChannel(t, instance.URL)
		defer connection.Close()
		defer channel.Close()
		dead := getRabbitMQDeliveryWithin(
			t,
			instance,
			channel,
			config.DeadLetterQueue,
			20*time.Second,
		)
		if dead.MessageId != eventID {
			t.Fatalf("dead-letter message id = %q, want %q", dead.MessageId, eventID)
		}
		if err := dead.Ack(false); err != nil {
			t.Fatalf("ack dead-letter inspection: %v", err)
		}
	})

	t.Run("consumer reconnects after broker restart", func(t *testing.T) {
		rabbitMQControl(t, instance, "stop_app")
		rabbitMQControl(t, instance, "start_app")
		waitForRabbitMQ(t, instance.URL, 30*time.Second)
		waitForConsumerQueue(t, instance.URL, config.Queue, int(config.LaneCount), 20*time.Second)
		eventID := "c4000000-0000-4000-8000-000000000001"
		publishDispatchEventually(t, instance.URL, config, eventID, 20*time.Second)
		observation := processor.waitFor(t, eventID, 20*time.Second)
		if observation.call != 1 {
			t.Fatalf("post-reconnect processor call = %d, want 1", observation.call)
		}
	})
}

func getRabbitMQDeliveryWithin(
	t *testing.T,
	instance rabbitmqcontainer.Instance,
	channel *amqp.Channel,
	queue string,
	timeout time.Duration,
) amqp.Delivery {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		delivery, found, err := channel.Get(queue, false)
		if err != nil {
			t.Fatalf("get RabbitMQ delivery: %v", err)
		}
		if found {
			return delivery
		}
		time.Sleep(25 * time.Millisecond)
	}
	queues := rabbitMQCommand(
		t,
		instance,
		"rabbitmqctl",
		"list_queues",
		"name",
		"messages_ready",
		"messages_unacknowledged",
		"policy",
		"arguments",
	)
	policies := rabbitMQCommand(t, instance, "rabbitmqctl", "list_policies", "--vhost", "/")
	bindings := rabbitMQCommand(
		t,
		instance,
		"rabbitmqctl",
		"list_bindings",
		"source_name",
		"destination_name",
		"routing_key",
	)
	t.Fatalf(
		"queue %q did not produce a delivery within %s\nqueues:\n%s\npolicies:\n%s\nbindings:\n%s",
		queue,
		timeout,
		queues,
		policies,
		bindings,
	)
	return amqp.Delivery{}
}

func applyConsumerReliabilityPolicy(t *testing.T, instance rabbitmqcontainer.Instance) {
	t.Helper()
	applyConsumerQueuePolicy(
		t,
		instance,
		"mail-dispatch-reliability",
		`^mail[.]dispatch[.]v1[.]q$`,
		mqcontract.RoutingDispatchDead,
	)
	applyConsumerQueuePolicy(
		t,
		instance,
		"mail-lifecycle-reliability",
		`^mail[.]lifecycle[.]v1[.]q$`,
		mqcontract.RoutingLifecycleDead,
	)
}

func applyConsumerQueuePolicy(
	t *testing.T,
	instance rabbitmqcontainer.Instance,
	name, pattern, deadLetterRoutingKey string,
) {
	t.Helper()
	definition := fmt.Sprintf(
		`{"dead-letter-exchange":%q,"dead-letter-routing-key":%q,"dead-letter-strategy":"at-least-once","overflow":"reject-publish","delivery-limit":20,"delayed-retry-type":"failed","delayed-retry-min":1000,"delayed-retry-max":30000}`,
		mqcontract.ExchangeDead,
		deadLetterRoutingKey,
	)
	rabbitMQCommand(
		t,
		instance,
		"rabbitmqctl",
		"set_policy",
		"--vhost",
		"/",
		name,
		pattern,
		definition,
		"--priority",
		"100",
		"--apply-to",
		"quorum_queues",
	)
}

func publishDispatchDelivery(
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
		config.RoutingKey,
		true,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				mqcontract.HeaderAggregateType:      "MAIL_MESSAGE",
				mqcontract.HeaderAggregateID:        "d0000000-0000-4000-8000-000000000001",
				mqcontract.HeaderAggregateSequence:  int64(2),
				mqcontract.HeaderDispatchGeneration: int64(1),
				mqcontract.HeaderPublishAttempt:     int64(1),
			},
			ContentType:   contentType,
			DeliveryMode:  amqp.Persistent,
			MessageId:     eventID,
			CorrelationId: "d0000000-0000-4000-8000-000000000001",
			Timestamp:     time.Now().UTC(),
			Type:          mqcontract.EventDispatchRequested,
			AppId:         config.ApplicationID,
			Body: []byte(`{
				"schema_version":1,
				"tenant_id":"d1000000-0000-4000-8000-000000000001",
				"message_id":"d0000000-0000-4000-8000-000000000001",
				"event_type":"MESSAGE_DISPATCH_REQUESTED",
				"occurred_at":"2026-08-09T16:00:00Z",
				"sequence":2,
				"dispatch_generation":1,
				"attempt_number":0
			}`),
		},
	)
	if err != nil {
		t.Fatalf("publish dispatch delivery: %v", err)
	}
}

func publishDispatchEventually(
	t *testing.T,
	brokerURL string,
	config consumerrabbit.Config,
	eventID string,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		connection, err := amqp.Dial(brokerURL)
		if err == nil {
			channel, channelErr := connection.Channel()
			if channelErr == nil {
				_ = channel.Close()
				_ = connection.Close()
				publishDispatchDelivery(t, brokerURL, config, eventID, true)
				return
			}
			_ = connection.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("could not publish dispatch delivery within %s", timeout)
}

func waitForConsumerQueue(
	t *testing.T,
	brokerURL string,
	queue string,
	wantConsumers int,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		connection, err := amqp.Dial(brokerURL)
		if err == nil {
			channel, channelErr := connection.Channel()
			if channelErr == nil {
				state, inspectErr := channel.QueueInspect(queue)
				_ = channel.Close()
				_ = connection.Close()
				if inspectErr == nil && state.Consumers >= wantConsumers {
					return
				}
			} else {
				_ = connection.Close()
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("consumer queue %q did not reach %d consumers", queue, wantConsumers)
}

func waitForQueueCount(t *testing.T, brokerURL, queue string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		connection, channel := openRabbitMQInspectionChannel(t, brokerURL)
		state, err := channel.QueueInspect(queue)
		_ = channel.Close()
		_ = connection.Close()
		if err == nil && state.Messages == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("queue %q did not reach %d ready messages", queue, want)
}

type integrationDispatchObservation struct {
	eventID string
	call    int
	at      time.Time
}

type integrationDispatchProcessor struct {
	mu       sync.Mutex
	calls    map[string]int
	failures map[string]int
	observed chan integrationDispatchObservation
}

func newIntegrationDispatchProcessor() *integrationDispatchProcessor {
	return &integrationDispatchProcessor{
		calls:    make(map[string]int),
		failures: make(map[string]int),
		observed: make(chan integrationDispatchObservation, 16),
	}
}

func (p *integrationDispatchProcessor) failOnce(eventID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures[eventID] = 1
}

func (p *integrationDispatchProcessor) Process(
	_ context.Context,
	command deliveryapp.DispatchCommand,
) (deliveryapp.DispatchResult, error) {
	p.mu.Lock()
	p.calls[command.EventID]++
	call := p.calls[command.EventID]
	shouldFail := p.failures[command.EventID] > 0
	if shouldFail {
		p.failures[command.EventID]--
	}
	p.mu.Unlock()
	p.observed <- integrationDispatchObservation{
		eventID: command.EventID,
		call:    call,
		at:      time.Now(),
	}
	if shouldFail {
		return deliveryapp.DispatchResult{}, errors.New("temporary integration failure")
	}
	return deliveryapp.DispatchResult{Disposition: deliveryapp.DispatchProviderAccepted}, nil
}

func (p *integrationDispatchProcessor) waitFor(
	t *testing.T,
	eventID string,
	timeout time.Duration,
) integrationDispatchObservation {
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
			t.Fatalf("processor did not observe event %q", eventID)
		}
	}
}
