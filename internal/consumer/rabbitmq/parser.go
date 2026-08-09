package rabbitmq

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	deliveryapp "github.com/Zhiruosama/Email-Service/internal/application/delivery"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	mqcontract "github.com/Zhiruosama/Email-Service/internal/messaging/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

const maxDeliveryBodyBytes = 64 * 1024

var ErrInvalidDelivery = errors.New("invalid RabbitMQ dispatch delivery")

type DeliveryValidationError struct {
	Code string
	err  error
}

func (e *DeliveryValidationError) Error() string {
	if e == nil {
		return ErrInvalidDelivery.Error()
	}
	return fmt.Sprintf("%s: %s", ErrInvalidDelivery, e.Code)
}

func (e *DeliveryValidationError) Unwrap() error { return ErrInvalidDelivery }

func (e *DeliveryValidationError) Cause() error {
	if e == nil {
		return nil
	}
	return e.err
}

type dispatchEnvelope struct {
	SchemaVersion      uint32    `json:"schema_version"`
	TenantID           string    `json:"tenant_id"`
	MessageID          string    `json:"message_id"`
	EventType          string    `json:"event_type"`
	OccurredAt         time.Time `json:"occurred_at"`
	Sequence           uint64    `json:"sequence"`
	DispatchGeneration uint64    `json:"dispatch_generation"`
}

func ParseDispatchDelivery(
	delivery amqp.Delivery,
	config Config,
) (deliveryapp.DispatchCommand, error) {
	if delivery.Exchange != config.Exchange || delivery.RoutingKey != config.RoutingKey {
		return deliveryapp.DispatchCommand{}, invalidDelivery("ROUTE_MISMATCH", nil)
	}
	if delivery.ContentType != mqcontract.ContentTypeJSON ||
		delivery.DeliveryMode != amqp.Persistent ||
		delivery.Type != mqcontract.EventDispatchRequested ||
		delivery.AppId != config.ApplicationID {
		return deliveryapp.DispatchCommand{}, invalidDelivery("PROPERTY_MISMATCH", nil)
	}
	if len(delivery.Body) == 0 || len(delivery.Body) > maxDeliveryBodyBytes {
		return deliveryapp.DispatchCommand{}, invalidDelivery("BODY_SIZE_INVALID", nil)
	}

	aggregateType, ok := stringHeader(delivery.Headers, mqcontract.HeaderAggregateType)
	if !ok || aggregateType != ports.OutboxAggregateMailMessage {
		return deliveryapp.DispatchCommand{}, invalidDelivery("AGGREGATE_TYPE_INVALID", nil)
	}
	aggregateID, ok := stringHeader(delivery.Headers, mqcontract.HeaderAggregateID)
	if !ok || aggregateID != delivery.CorrelationId {
		return deliveryapp.DispatchCommand{}, invalidDelivery("AGGREGATE_ID_MISMATCH", nil)
	}
	sequence, ok := positiveLongHeader(delivery.Headers, mqcontract.HeaderAggregateSequence)
	if !ok {
		return deliveryapp.DispatchCommand{}, invalidDelivery("SEQUENCE_HEADER_INVALID", nil)
	}
	generation, ok := positiveLongHeader(delivery.Headers, mqcontract.HeaderDispatchGeneration)
	if !ok {
		return deliveryapp.DispatchCommand{}, invalidDelivery("GENERATION_HEADER_INVALID", nil)
	}
	if _, ok := positiveLongHeader(delivery.Headers, mqcontract.HeaderPublishAttempt); !ok {
		return deliveryapp.DispatchCommand{}, invalidDelivery("PUBLISH_ATTEMPT_HEADER_INVALID", nil)
	}

	var envelope dispatchEnvelope
	decoder := json.NewDecoder(bytes.NewReader(delivery.Body))
	if err := decoder.Decode(&envelope); err != nil {
		return deliveryapp.DispatchCommand{}, invalidDelivery("BODY_JSON_INVALID", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return deliveryapp.DispatchCommand{}, invalidDelivery("BODY_JSON_TRAILING", err)
	}
	if envelope.SchemaVersion != 1 || envelope.OccurredAt.IsZero() ||
		envelope.EventType != mqcontract.EventDispatchRequested {
		return deliveryapp.DispatchCommand{}, invalidDelivery("ENVELOPE_INVALID", nil)
	}
	if envelope.MessageID != aggregateID || envelope.Sequence != sequence ||
		envelope.DispatchGeneration != generation {
		return deliveryapp.DispatchCommand{}, invalidDelivery("ENVELOPE_HEADER_MISMATCH", nil)
	}

	command := deliveryapp.DispatchCommand{
		EventID:            delivery.MessageId,
		TenantID:           envelope.TenantID,
		MessageID:          envelope.MessageID,
		AggregateSequence:  envelope.Sequence,
		DispatchGeneration: envelope.DispatchGeneration,
	}
	if err := command.Validate(); err != nil {
		return deliveryapp.DispatchCommand{}, invalidDelivery("COMMAND_INVALID", err)
	}
	return command, nil
}

func stringHeader(headers amqp.Table, name string) (string, bool) {
	value, ok := headers[name].(string)
	return value, ok && value != ""
}

func positiveLongHeader(headers amqp.Table, name string) (uint64, bool) {
	value, ok := headers[name].(int64)
	if !ok || value <= 0 {
		return 0, false
	}
	return uint64(value), true
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple JSON values")
	}
	return err
}

func invalidDelivery(code string, cause error) error {
	return &DeliveryValidationError{Code: code, err: cause}
}
