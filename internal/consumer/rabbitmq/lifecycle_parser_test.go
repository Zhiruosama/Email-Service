package rabbitmq

import (
	"errors"
	"testing"

	mqcontract "github.com/Zhiruosama/Email-Service/internal/messaging/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestParseLifecycleDelivery(t *testing.T) {
	t.Parallel()
	config := DefaultLifecycleConfig("amqp://guest:guest@localhost:5672/", "parser")
	for _, eventType := range []string{
		mqcontract.EventMessageAccepted,
		mqcontract.EventStatusChanged,
	} {
		delivery := validLifecycleDelivery(config, eventType)
		command, err := ParseLifecycleDelivery(delivery, config)
		if err != nil {
			t.Fatalf("valid %s delivery rejected: %v", eventType, err)
		}
		if command.EventID != delivery.MessageId {
			t.Fatalf("parsed command = %#v", command)
		}
	}

	base := validLifecycleDelivery(config, mqcontract.EventStatusChanged)
	tests := []struct {
		name string
		code string
		edit func(*amqp.Delivery)
	}{
		{name: "route", code: "ROUTE_MISMATCH", edit: func(d *amqp.Delivery) { d.RoutingKey = config.RoutingKey }},
		{name: "unknown event", code: "ROUTE_MISMATCH", edit: func(d *amqp.Delivery) { d.Type = "UNKNOWN" }},
		{name: "property", code: "PROPERTY_MISMATCH", edit: func(d *amqp.Delivery) { d.AppId = "other" }},
		{name: "body size", code: "BODY_SIZE_INVALID", edit: func(d *amqp.Delivery) { d.Body = nil }},
		{name: "aggregate type", code: "AGGREGATE_TYPE_INVALID", edit: func(d *amqp.Delivery) {
			d.Headers[mqcontract.HeaderAggregateType] = "OTHER"
		}},
		{name: "aggregate id", code: "AGGREGATE_ID_MISMATCH", edit: func(d *amqp.Delivery) {
			d.Headers[mqcontract.HeaderAggregateID] = "f0000000-0000-4000-8000-000000000099"
		}},
		{name: "sequence", code: "SEQUENCE_HEADER_INVALID", edit: func(d *amqp.Delivery) {
			d.Headers[mqcontract.HeaderAggregateSequence] = int64(0)
		}},
		{name: "generation", code: "GENERATION_HEADER_INVALID", edit: func(d *amqp.Delivery) {
			d.Headers[mqcontract.HeaderDispatchGeneration] = int64(-1)
		}},
		{name: "publish attempt", code: "PUBLISH_ATTEMPT_HEADER_INVALID", edit: func(d *amqp.Delivery) {
			delete(d.Headers, mqcontract.HeaderPublishAttempt)
		}},
		{name: "JSON", code: "BODY_JSON_INVALID", edit: func(d *amqp.Delivery) { d.Body = []byte(`{"`) }},
		{name: "trailing JSON", code: "BODY_JSON_TRAILING", edit: func(d *amqp.Delivery) {
			d.Body = append(d.Body, []byte(` {}`)...)
		}},
		{name: "envelope", code: "ENVELOPE_INVALID", edit: func(d *amqp.Delivery) {
			d.Body = []byte(`{"schema_version":2}`)
		}},
		{name: "envelope mismatch", code: "ENVELOPE_HEADER_MISMATCH", edit: func(d *amqp.Delivery) {
			d.Headers[mqcontract.HeaderAggregateSequence] = int64(3)
		}},
		{name: "command", code: "COMMAND_INVALID", edit: func(d *amqp.Delivery) { d.MessageId = "invalid" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneDelivery(base)
			test.edit(&candidate)
			_, err := ParseLifecycleDelivery(candidate, config)
			if !errors.Is(err, ErrInvalidDelivery) {
				t.Fatalf("error = %v, want ErrInvalidDelivery", err)
			}
			var validation *DeliveryValidationError
			if !errors.As(err, &validation) || validation.Code != test.code {
				t.Fatalf("validation error = %#v, want code %q", validation, test.code)
			}
		})
	}
}

func validLifecycleDelivery(config Config, eventType string) amqp.Delivery {
	routingKey := mqcontract.RoutingStatusChanged
	sequence := int64(2)
	generation := int64(1)
	body := []byte(`{
		"schema_version":1,
		"tenant_id":"f1000000-0000-4000-8000-000000000001",
		"message_id":"f0000000-0000-4000-8000-000000000001",
		"event_type":"MESSAGE_STATUS_CHANGED",
		"from":"ACCEPTED",
		"to":"QUEUED",
		"occurred_at":"2026-08-09T16:00:00Z",
		"sequence":2,
		"dispatch_generation":1,
		"attempt_number":0
	}`)
	if eventType == mqcontract.EventMessageAccepted {
		routingKey = mqcontract.RoutingMessageAccepted
		sequence = 1
		generation = 0
		body = []byte(`{
			"schema_version":1,
			"tenant_id":"f1000000-0000-4000-8000-000000000001",
			"message_id":"f0000000-0000-4000-8000-000000000001",
			"event_type":"MESSAGE_ACCEPTED",
			"to":"ACCEPTED",
			"occurred_at":"2026-08-09T16:00:00Z",
			"sequence":1,
			"dispatch_generation":0,
			"attempt_number":0
		}`)
	}
	return amqp.Delivery{
		Headers: amqp.Table{
			mqcontract.HeaderAggregateType:      "MAIL_MESSAGE",
			mqcontract.HeaderAggregateID:        "f0000000-0000-4000-8000-000000000001",
			mqcontract.HeaderAggregateSequence:  sequence,
			mqcontract.HeaderDispatchGeneration: generation,
			mqcontract.HeaderPublishAttempt:     int64(1),
		},
		ContentType:   mqcontract.ContentTypeJSON,
		DeliveryMode:  amqp.Persistent,
		MessageId:     "f2000000-0000-4000-8000-000000000001",
		CorrelationId: "f0000000-0000-4000-8000-000000000001",
		Type:          eventType,
		AppId:         config.ApplicationID,
		Body:          body,
		Exchange:      config.Exchange,
		RoutingKey:    routingKey,
	}
}
