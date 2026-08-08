//go:build integration

package integration_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	rabbitpublisher "github.com/Zhiruosama/Email-Service/internal/publisher/rabbitmq"
	postgresstore "github.com/Zhiruosama/Email-Service/internal/storage/postgres"
	"github.com/Zhiruosama/Email-Service/internal/testkit/rabbitmqcontainer"
	amqp "github.com/rabbitmq/amqp091-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestRabbitMQPublisher(t *testing.T) {
	instance := rabbitmqcontainer.Start(t)
	version := strings.TrimSpace(rabbitMQCommand(
		t,
		instance,
		"rabbitmq-diagnostics",
		"-q",
		"server_version",
	))
	if !strings.HasPrefix(version, "4.3.") {
		t.Fatalf("RabbitMQ version = %q, want 4.3.x", version)
	}

	t.Run("declares quorum topology and confirms persistent message", func(t *testing.T) {
		config := rabbitMQIntegrationConfig(instance.URL, "confirmed")
		publisher := newRabbitMQIntegrationPublisher(t, config)
		publication := rabbitMQPublication("a1000000-0000-4000-8000-000000000001")

		publishCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := publisher.Publish(publishCtx, publication); err != nil {
			t.Fatalf("publish confirmed message: %v", err)
		}

		connection, channel := openRabbitMQInspectionChannel(t, instance.URL)
		defer connection.Close()
		defer channel.Close()
		queue := config.Queues[0]
		if _, err := channel.QueueDeclarePassive(
			queue.Name,
			true,
			false,
			false,
			false,
			amqp.Table{"x-queue-type": "quorum"},
		); err != nil {
			t.Fatalf("passively verify durable quorum queue: %v", err)
		}

		delivery := getRabbitMQDelivery(t, channel, queue.Name)
		assertRabbitMQDelivery(t, delivery, publication)
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("ack inspected delivery: %v", err)
		}
	})

	t.Run("mandatory return is permanent unroutable failure", func(t *testing.T) {
		config := rabbitMQIntegrationConfig(instance.URL, "unroutable")
		publisher := newRabbitMQIntegrationPublisher(t, config)
		first := rabbitMQPublication("a2000000-0000-4000-8000-000000000001")
		if err := publisher.Publish(context.Background(), first); err != nil {
			t.Fatalf("initial publish: %v", err)
		}

		connection, channel := openRabbitMQInspectionChannel(t, instance.URL)
		defer connection.Close()
		defer channel.Close()
		queue := config.Queues[0]
		if err := channel.QueueUnbind(
			queue.Name,
			rabbitpublisher.RoutingKeyDispatchRequested,
			config.Exchange,
			nil,
		); err != nil {
			t.Fatalf("remove binding: %v", err)
		}
		firstDelivery := getRabbitMQDelivery(t, channel, queue.Name)
		if err := firstDelivery.Ack(false); err != nil {
			t.Fatalf("ack initial delivery: %v", err)
		}

		unroutable := rabbitMQPublication("a2000000-0000-4000-8000-000000000002")
		err := publisher.Publish(context.Background(), unroutable)
		assertRabbitMQPublishError(
			t,
			err,
			rabbitpublisher.ErrorCodeUnroutable,
			false,
		)
	})

	t.Run("duplicate event id remains visible to consumer", func(t *testing.T) {
		config := rabbitMQIntegrationConfig(instance.URL, "duplicate")
		publisher := newRabbitMQIntegrationPublisher(t, config)
		publication := rabbitMQPublication("a3000000-0000-4000-8000-000000000001")
		if err := publisher.Publish(context.Background(), publication); err != nil {
			t.Fatalf("first duplicate publish: %v", err)
		}
		publication.AttemptNumber++
		if err := publisher.Publish(context.Background(), publication); err != nil {
			t.Fatalf("second duplicate publish: %v", err)
		}

		connection, channel := openRabbitMQInspectionChannel(t, instance.URL)
		defer connection.Close()
		defer channel.Close()
		for index := range 2 {
			delivery := getRabbitMQDelivery(t, channel, config.Queues[0].Name)
			if delivery.MessageId != publication.Event.ID {
				t.Fatalf("duplicate %d message id = %q", index, delivery.MessageId)
			}
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("ack duplicate %d: %v", index, err)
			}
		}
	})

	t.Run("relay marks outbox published after broker confirm", func(t *testing.T) {
		pool, _ := setupMessageRepository(t)
		transactor := postgresstore.NewTransactionManager(pool)
		config := rabbitMQIntegrationConfig(instance.URL, "relay")
		publisher := newRabbitMQIntegrationPublisher(t, config)
		event := relayOutboxEvent(
			"a3500000-0000-4000-8000-000000000001",
			"b3500000-0000-4000-8000-000000000001",
			"MESSAGE_DISPATCH_REQUESTED",
		)
		appendRelayOutboxEvents(t, context.Background(), transactor, event)
		relay := mustOutboxRelay(t, transactor, publisher, 1, 3, 5*time.Second)

		result, err := relay.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("run Relay with RabbitMQ Publisher: %v", err)
		}
		if result.Claimed != 1 || result.Published != 1 {
			t.Fatalf("Relay result = %#v, want one confirmed publication", result)
		}
		assertRelayOutboxState(
			t,
			context.Background(),
			pool,
			event.ID,
			"PUBLISHED",
			1,
			"",
			"",
			true,
		)

		connection, channel := openRabbitMQInspectionChannel(t, instance.URL)
		defer connection.Close()
		defer channel.Close()
		delivery := getRabbitMQDelivery(t, channel, config.Queues[0].Name)
		if delivery.MessageId != event.ID {
			t.Fatalf("Relay delivery event id = %q, want %q", delivery.MessageId, event.ID)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("ack Relay delivery: %v", err)
		}
	})

	t.Run("persistent message survives restart and publisher redials", func(t *testing.T) {
		config := rabbitMQIntegrationConfig(instance.URL, "restart")
		config.ConnectTimeout = time.Second
		publisher := newRabbitMQIntegrationPublisher(t, config)
		beforeRestart := rabbitMQPublication("a4000000-0000-4000-8000-000000000001")
		if err := publisher.Publish(context.Background(), beforeRestart); err != nil {
			t.Fatalf("publish before restart: %v", err)
		}

		rabbitMQControl(t, instance, "stop_app")

		duringRestart := rabbitMQPublication("a4000000-0000-4000-8000-000000000002")
		publishCtx, publishCancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := publisher.Publish(publishCtx, duringRestart)
		publishCancel()
		if err == nil {
			t.Fatal("publish while RabbitMQ was stopped unexpectedly succeeded")
		}
		var failure *ports.OutboxPublishError
		if !errors.As(err, &failure) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stopped broker error = %T %v", err, err)
		}
		if failure != nil && !failure.Retryable {
			t.Fatalf("stopped broker failure is permanent: %v", failure)
		}

		rabbitMQControl(t, instance, "start_app")
		waitForRabbitMQ(t, instance.URL, 30*time.Second)

		afterRestart := rabbitMQPublication("a4000000-0000-4000-8000-000000000003")
		publishRabbitMQEventually(t, publisher, afterRestart, 20*time.Second)

		connection, channel := openRabbitMQInspectionChannel(t, instance.URL)
		defer connection.Close()
		defer channel.Close()
		messageIDs := map[string]bool{}
		for range 2 {
			delivery := getRabbitMQDelivery(t, channel, config.Queues[0].Name)
			messageIDs[delivery.MessageId] = true
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("ack restart delivery: %v", err)
			}
		}
		if !messageIDs[beforeRestart.Event.ID] || !messageIDs[afterRestart.Event.ID] {
			t.Fatalf("messages after restart = %#v", messageIDs)
		}
	})
}

func rabbitMQIntegrationConfig(brokerURL, suffix string) rabbitpublisher.Config {
	config := rabbitpublisher.DefaultConfig(brokerURL, "integration-"+suffix)
	config.Exchange = "mail.events.integration." + suffix
	config.ChannelPoolSize = 2
	config.Routes = map[string]string{
		"MESSAGE_DISPATCH_REQUESTED": rabbitpublisher.RoutingKeyDispatchRequested,
	}
	config.Queues = []rabbitpublisher.QueueTopology{{
		Name:        "mail.dispatch.integration." + suffix + ".q",
		BindingKeys: []string{rabbitpublisher.RoutingKeyDispatchRequested},
	}}
	return config
}

func newRabbitMQIntegrationPublisher(
	t *testing.T,
	config rabbitpublisher.Config,
) *rabbitpublisher.Publisher {
	t.Helper()
	publisher, err := rabbitpublisher.New(config)
	if err != nil {
		t.Fatalf("create RabbitMQ publisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("close RabbitMQ publisher: %v", err)
		}
	})
	return publisher
}

func rabbitMQPublication(eventID string) ports.OutboxPublication {
	return ports.OutboxPublication{
		Event: ports.OutboxEvent{
			ID:                 eventID,
			AggregateType:      ports.OutboxAggregateMailMessage,
			AggregateID:        "b0000000-0000-4000-8000-000000000001",
			EventType:          "MESSAGE_DISPATCH_REQUESTED",
			AggregateSequence:  2,
			DispatchGeneration: 1,
			Payload:            []byte(`{"schema_version":1,"message_id":"b0000000-0000-4000-8000-000000000001"}`),
		},
		AttemptNumber: 1,
	}
}

func openRabbitMQInspectionChannel(
	t *testing.T,
	brokerURL string,
) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	connection, err := amqp.Dial(brokerURL)
	if err != nil {
		t.Fatalf("dial RabbitMQ for inspection: %v", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		t.Fatalf("open RabbitMQ inspection channel: %v", err)
	}
	return connection, channel
}

func getRabbitMQDelivery(t *testing.T, channel *amqp.Channel, queue string) amqp.Delivery {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		delivery, found, err := channel.Get(queue, false)
		if err != nil {
			t.Fatalf("get RabbitMQ delivery: %v", err)
		}
		if found {
			return delivery
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("queue %q did not produce a delivery", queue)
	return amqp.Delivery{}
}

func assertRabbitMQDelivery(
	t *testing.T,
	delivery amqp.Delivery,
	publication ports.OutboxPublication,
) {
	t.Helper()
	if delivery.MessageId != publication.Event.ID ||
		delivery.CorrelationId != publication.Event.AggregateID ||
		delivery.Type != publication.Event.EventType {
		t.Fatalf("delivery properties = %#v", delivery)
	}
	if delivery.DeliveryMode != amqp.Persistent || delivery.ContentType != "application/json" {
		t.Fatalf("delivery persistence metadata = %#v", delivery)
	}
	if string(delivery.Body) != string(publication.Event.Payload) {
		t.Fatalf("delivery body = %s", delivery.Body)
	}
	if value := delivery.Headers["x-mail-aggregate-sequence"]; value != int64(2) {
		t.Fatalf("aggregate sequence header = %#v", value)
	}
	if value := delivery.Headers["x-mail-dispatch-generation"]; value != int64(1) {
		t.Fatalf("dispatch generation header = %#v", value)
	}
}

func assertRabbitMQPublishError(
	t *testing.T,
	err error,
	code string,
	retryable bool,
) {
	t.Helper()
	var failure *ports.OutboxPublishError
	if !errors.As(err, &failure) {
		t.Fatalf("publish error type = %T, want *OutboxPublishError: %v", err, err)
	}
	if failure.Code != code || failure.Retryable != retryable {
		t.Fatalf("failure = %q/%t, want %q/%t", failure.Code, failure.Retryable, code, retryable)
	}
}

func publishRabbitMQEventually(
	t *testing.T,
	publisher *rabbitpublisher.Publisher,
	publication ports.OutboxPublication,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		lastErr = publisher.Publish(ctx, publication)
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("publish after RabbitMQ restart: %v", lastErr)
}

func waitForRabbitMQ(t *testing.T, brokerURL string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		connection, err := amqp.Dial(brokerURL)
		if err == nil {
			_ = connection.Close()
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("RabbitMQ did not become ready: %v", lastErr)
}

func rabbitMQControl(
	t *testing.T,
	instance rabbitmqcontainer.Instance,
	command string,
) {
	t.Helper()
	_ = rabbitMQCommand(t, instance, "rabbitmqctl", command)
}

func rabbitMQCommand(
	t *testing.T,
	instance rabbitmqcontainer.Instance,
	command ...string,
) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	exitCode, output, err := instance.Container.Exec(ctx, command, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("RabbitMQ command %q: %v", command, err)
	}
	body, readErr := io.ReadAll(output)
	if readErr != nil {
		t.Fatalf("read RabbitMQ command %q output: %v", command, readErr)
	}
	if exitCode != 0 {
		t.Fatalf("RabbitMQ command %q exit %d: %s", command, exitCode, body)
	}
	return string(body)
}
