// Package notification delivers sanitized lifecycle facts to registered
// subscribers without depending on RabbitMQ or gRPC transport details.
package notification

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/google/uuid"
)

var (
	ErrInvalidCommand        = errors.New("invalid notification command")
	ErrInvalidWorkerConfig   = errors.New("invalid notification worker configuration")
	ErrJournalEventMissing   = errors.New("notification references a missing journal event")
	ErrNotificationInvariant = errors.New("notification worker invariant violation")
)

const maxCallbackTimeout = time.Minute

type Command struct {
	EventID string
}

func (c Command) Validate() error {
	if _, err := uuid.Parse(c.EventID); err != nil {
		return fmt.Errorf("%w: event id must be a UUID", ErrInvalidCommand)
	}
	return nil
}

type WorkerConfig struct {
	CallbackTimeout time.Duration
}

func (c WorkerConfig) Validate() error {
	if c.CallbackTimeout <= 0 || c.CallbackTimeout > maxCallbackTimeout {
		return fmt.Errorf(
			"%w: callback timeout must be in range (0, %s]",
			ErrInvalidWorkerConfig,
			maxCallbackTimeout,
		)
	}
	return nil
}

type Result struct {
	Disposition ports.EventAckDisposition
}

type ErrorClass string

const (
	ErrorTransient ErrorClass = "TRANSIENT"
	ErrorPoison    ErrorClass = "POISON"
)

// ClassifyError gives the future RabbitMQ adapter a stable ACK decision.
// Unknown failures are retryable because storage or transport outages usually
// recover; only malformed commands, broken local facts, or an explicitly
// permanent callback rejection are dead-lettered immediately.
func ClassifyError(err error) ErrorClass {
	if errors.Is(err, ErrInvalidCommand) ||
		errors.Is(err, ErrJournalEventMissing) ||
		errors.Is(err, ErrNotificationInvariant) ||
		errors.Is(err, ports.ErrInvalidDeliveryEvent) ||
		errors.Is(err, ports.ErrCorruptDeliveryEvent) {
		return ErrorPoison
	}
	var subscriberErr *ports.DeliveryEventSubscriberError
	if errors.As(err, &subscriberErr) {
		if validationErr := subscriberErr.Validate(); validationErr != nil {
			return ErrorPoison
		}
		if !subscriberErr.Retryable {
			return ErrorPoison
		}
	}
	return ErrorTransient
}

type Worker struct {
	events     ports.DeliveryEventReader
	subscriber ports.DeliveryEventSubscriber
	config     WorkerConfig
}

func NewWorker(
	events ports.DeliveryEventReader,
	subscriber ports.DeliveryEventSubscriber,
	config WorkerConfig,
) (*Worker, error) {
	if events == nil {
		panic("notification: nil delivery event reader")
	}
	if subscriber == nil {
		panic("notification: nil delivery event subscriber")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Worker{events: events, subscriber: subscriber, config: config}, nil
}

func (w *Worker) Process(ctx context.Context, command Command) (Result, error) {
	if err := command.Validate(); err != nil {
		return Result{}, err
	}
	event, err := w.events.GetByID(ctx, command.EventID)
	if errors.Is(err, ports.ErrDeliveryEventNotFound) {
		return Result{}, fmt.Errorf("%w: %s", ErrJournalEventMissing, command.EventID)
	}
	if err != nil {
		return Result{}, err
	}
	if event.ID != command.EventID {
		return Result{}, fmt.Errorf(
			"%w: journal returned another event id",
			ErrNotificationInvariant,
		)
	}
	if err := event.Validate(); err != nil {
		return Result{}, fmt.Errorf(
			"%w: journal returned an invalid event: %v",
			ErrNotificationInvariant,
			err,
		)
	}

	callbackCtx, cancel := context.WithTimeout(ctx, w.config.CallbackTimeout)
	disposition, callbackErr := w.subscriber.Report(callbackCtx, event)
	callbackContextErr := callbackCtx.Err()
	cancel()
	if callbackErr != nil {
		var subscriberErr *ports.DeliveryEventSubscriberError
		if errors.As(callbackErr, &subscriberErr) {
			if err := subscriberErr.Validate(); err != nil {
				return Result{}, fmt.Errorf(
					"%w: subscriber returned an invalid typed error",
					ErrNotificationInvariant,
				)
			}
			return Result{}, subscriberErr
		}
		if callbackContextErr != nil {
			code := "CALLBACK_CANCELED"
			if errors.Is(callbackContextErr, context.DeadlineExceeded) {
				code = "CALLBACK_TIMEOUT"
			}
			return Result{}, ports.NewDeliveryEventSubscriberError(
				code,
				true,
				callbackErr,
			)
		}
		// Adapters are expected to return a typed error. Treat an untyped error
		// as transient so a temporary dependency bug does not lose an event.
		return Result{}, ports.NewDeliveryEventSubscriberError(
			"CALLBACK_INTERNAL",
			true,
			callbackErr,
		)
	}
	if !disposition.Valid() {
		return Result{}, fmt.Errorf(
			"%w: subscriber returned unknown disposition %q",
			ErrNotificationInvariant,
			disposition,
		)
	}
	return Result{Disposition: disposition}, nil
}
