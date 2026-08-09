package rabbitmq

import (
	"errors"
	"testing"
	"time"

	mqcontract "github.com/Zhiruosama/Email-Service/internal/messaging/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestParseDispatchDelivery(t *testing.T) {
	t.Parallel()
	config := DefaultConfig("amqp://guest:guest@localhost:5672/", "parser")
	delivery := validDispatchDelivery(config)

	command, err := ParseDispatchDelivery(delivery, config)
	if err != nil {
		t.Fatalf("valid delivery rejected: %v", err)
	}
	if command.EventID != delivery.MessageId ||
		command.MessageID != delivery.CorrelationId ||
		command.AggregateSequence != 2 ||
		command.DispatchGeneration != 1 {
		t.Fatalf("parsed command = %#v", command)
	}

	tests := []struct {
		name string
		code string
		edit func(*amqp.Delivery)
	}{
		{name: "route", code: "ROUTE_MISMATCH", edit: func(d *amqp.Delivery) { d.RoutingKey = "wrong" }},
		{name: "property", code: "PROPERTY_MISMATCH", edit: func(d *amqp.Delivery) { d.AppId = "other" }},
		{name: "body size", code: "BODY_SIZE_INVALID", edit: func(d *amqp.Delivery) { d.Body = nil }},
		{name: "aggregate type", code: "AGGREGATE_TYPE_INVALID", edit: func(d *amqp.Delivery) {
			d.Headers[mqcontract.HeaderAggregateType] = "OTHER"
		}},
		{name: "aggregate id", code: "AGGREGATE_ID_MISMATCH", edit: func(d *amqp.Delivery) {
			d.Headers[mqcontract.HeaderAggregateID] = "50000000-0000-4000-8000-000000000099"
		}},
		{name: "sequence header type", code: "SEQUENCE_HEADER_INVALID", edit: func(d *amqp.Delivery) {
			d.Headers[mqcontract.HeaderAggregateSequence] = int32(2)
		}},
		{name: "generation", code: "GENERATION_HEADER_INVALID", edit: func(d *amqp.Delivery) {
			d.Headers[mqcontract.HeaderDispatchGeneration] = int64(0)
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
			candidate := cloneDelivery(delivery)
			test.edit(&candidate)
			_, err := ParseDispatchDelivery(candidate, config)
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

func validDispatchDelivery(config Config) amqp.Delivery {
	return amqp.Delivery{
		Headers: amqp.Table{
			mqcontract.HeaderAggregateType:      "MAIL_MESSAGE",
			mqcontract.HeaderAggregateID:        "50000000-0000-4000-8000-000000000001",
			mqcontract.HeaderAggregateSequence:  int64(2),
			mqcontract.HeaderDispatchGeneration: int64(1),
			mqcontract.HeaderPublishAttempt:     int64(1),
		},
		ContentType:   mqcontract.ContentTypeJSON,
		DeliveryMode:  amqp.Persistent,
		MessageId:     "51000000-0000-4000-8000-000000000001",
		CorrelationId: "50000000-0000-4000-8000-000000000001",
		Timestamp:     time.Date(2026, 8, 9, 14, 0, 0, 0, time.UTC),
		Type:          mqcontract.EventDispatchRequested,
		AppId:         config.ApplicationID,
		Body: []byte(`{
			"schema_version":1,
			"tenant_id":"52000000-0000-4000-8000-000000000001",
			"message_id":"50000000-0000-4000-8000-000000000001",
			"event_type":"MESSAGE_DISPATCH_REQUESTED",
			"occurred_at":"2026-08-09T14:00:00Z",
			"sequence":2,
			"dispatch_generation":1,
			"attempt_number":0
		}`),
		Exchange:   config.Exchange,
		RoutingKey: config.RoutingKey,
	}
}

func cloneDelivery(delivery amqp.Delivery) amqp.Delivery {
	cloned := delivery
	cloned.Body = append([]byte(nil), delivery.Body...)
	cloned.Headers = make(amqp.Table, len(delivery.Headers))
	for name, value := range delivery.Headers {
		cloned.Headers[name] = value
	}
	return cloned
}
