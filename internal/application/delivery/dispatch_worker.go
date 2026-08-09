package delivery

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"github.com/google/uuid"
)

var (
	ErrInvalidDispatchCommand      = errors.New("invalid dispatch command")
	ErrInvalidDispatchWorkerConfig = errors.New("invalid dispatch worker configuration")
	ErrDispatchMessageMissing      = errors.New("dispatch command references a missing message")
	ErrDispatchInvariant           = errors.New("dispatch worker invariant violation")
	ErrInvalidDeliveryRetryDelay   = errors.New("invalid delivery retry delay")
)

const (
	providerTimeoutUnknownCode  = "PROVIDER_TIMEOUT_UNKNOWN"
	providerCanceledUnknownCode = "PROVIDER_CANCELED_UNKNOWN"
	maxProviderTimeout          = 10 * time.Minute
	maxFinalizeTimeout          = time.Minute
)

type DispatchCommand struct {
	EventID            string
	TenantID           string
	MessageID          string
	AggregateSequence  uint64
	DispatchGeneration uint64
}

func (c DispatchCommand) Validate() error {
	if _, err := uuid.Parse(c.EventID); err != nil {
		return fmt.Errorf("%w: event id must be a UUID", ErrInvalidDispatchCommand)
	}
	if _, err := uuid.Parse(c.TenantID); err != nil {
		return fmt.Errorf("%w: tenant id must be a UUID", ErrInvalidDispatchCommand)
	}
	if _, err := uuid.Parse(c.MessageID); err != nil {
		return fmt.Errorf("%w: message id must be a UUID", ErrInvalidDispatchCommand)
	}
	if c.AggregateSequence == 0 || c.AggregateSequence > math.MaxInt64 {
		return fmt.Errorf("%w: aggregate sequence must fit a positive PostgreSQL BIGINT", ErrInvalidDispatchCommand)
	}
	if c.DispatchGeneration == 0 || c.DispatchGeneration > math.MaxInt64 {
		return fmt.Errorf("%w: dispatch generation must fit a positive PostgreSQL BIGINT", ErrInvalidDispatchCommand)
	}
	return nil
}

type DispatchWorkerConfig struct {
	ProviderTimeout time.Duration
	FinalizeTimeout time.Duration
}

func (c DispatchWorkerConfig) Validate() error {
	if c.ProviderTimeout <= 0 || c.ProviderTimeout > maxProviderTimeout {
		return fmt.Errorf(
			"%w: provider timeout must be in range (0, %s]",
			ErrInvalidDispatchWorkerConfig,
			maxProviderTimeout,
		)
	}
	if c.FinalizeTimeout <= 0 || c.FinalizeTimeout > maxFinalizeTimeout {
		return fmt.Errorf(
			"%w: finalize timeout must be in range (0, %s]",
			ErrInvalidDispatchWorkerConfig,
			maxFinalizeTimeout,
		)
	}
	return nil
}

type DispatchDisposition string

const (
	DispatchProviderAccepted  DispatchDisposition = "PROVIDER_ACCEPTED"
	DispatchRetryScheduled    DispatchDisposition = "RETRY_SCHEDULED"
	DispatchPermanentlyFailed DispatchDisposition = "PERMANENTLY_FAILED"
	DispatchSubmissionUnknown DispatchDisposition = "SUBMISSION_UNKNOWN"
	DispatchDeadLettered      DispatchDisposition = "DEAD_LETTERED"
	DispatchExpired           DispatchDisposition = "EXPIRED"
	DispatchDuplicate         DispatchDisposition = "DUPLICATE"
	DispatchStale             DispatchDisposition = "STALE"
)

type DispatchResult struct {
	Disposition   DispatchDisposition
	AttemptID     string
	AttemptNumber uint32
}

type DispatchErrorClass string

const (
	DispatchErrorTransient DispatchErrorClass = "TRANSIENT"
	DispatchErrorPoison    DispatchErrorClass = "POISON"
)

// ClassifyDispatchError gives a transport adapter a stable retry decision.
// Unknown infrastructure errors are transient by default; errors proving that
// the command or local invariant cannot become valid through redelivery are
// poison and should be dead-lettered.
func ClassifyDispatchError(err error) DispatchErrorClass {
	if errors.Is(err, ErrInvalidDispatchCommand) ||
		errors.Is(err, ErrDispatchMessageMissing) ||
		errors.Is(err, ErrDispatchInvariant) ||
		errors.Is(err, ErrInvalidDeliveryRetryDelay) ||
		errors.Is(err, ErrMessageEventMapping) ||
		errors.Is(err, ErrNoPendingMessageEvents) ||
		errors.Is(err, ports.ErrInvalidMessageRecord) ||
		errors.Is(err, ports.ErrInvalidDeliveryEvent) ||
		errors.Is(err, ports.ErrDeliveryEventConflict) ||
		errors.Is(err, ports.ErrInvalidDeliveryAttempt) ||
		errors.Is(err, ports.ErrInvalidProviderRequest) ||
		errors.Is(err, ports.ErrInvalidProviderResult) {
		return DispatchErrorPoison
	}
	return DispatchErrorTransient
}

// DispatchWorker executes one logical MQ dispatch command. RabbitMQ delivery
// tags and ACK APIs deliberately remain in the transport adapter.
type DispatchWorker struct {
	transactor ports.Transactor
	provider   ports.EmailProvider
	retry      DeliveryRetryPolicy
	config     DispatchWorkerConfig
}

func NewDispatchWorker(
	transactor ports.Transactor,
	provider ports.EmailProvider,
	retryPolicy DeliveryRetryPolicy,
	config DispatchWorkerConfig,
) (*DispatchWorker, error) {
	if transactor == nil {
		panic("delivery: nil transactor")
	}
	if provider == nil {
		panic("delivery: nil email provider")
	}
	if retryPolicy == nil {
		panic("delivery: nil delivery retry policy")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := ports.ValidateProviderKey(provider.Key()); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDispatchWorkerConfig, err)
	}
	return &DispatchWorker{
		transactor: transactor,
		provider:   provider,
		retry:      retryPolicy,
		config:     config,
	}, nil
}

func (w *DispatchWorker) Process(
	ctx context.Context,
	command DispatchCommand,
) (DispatchResult, error) {
	if err := command.Validate(); err != nil {
		return DispatchResult{}, err
	}
	claim, err := w.claim(ctx, command)
	if err != nil {
		return DispatchResult{}, err
	}
	if claim.resolved != nil {
		return *claim.resolved, nil
	}

	providerCtx, cancelProvider := context.WithTimeout(ctx, w.config.ProviderTimeout)
	providerResult := w.provider.Submit(providerCtx, claim.request)
	providerContextErr := providerCtx.Err()
	cancelProvider()

	if err := providerResult.Validate(); err != nil {
		if providerContextErr != nil {
			code := providerCanceledUnknownCode
			if errors.Is(providerContextErr, context.DeadlineExceeded) {
				code = providerTimeoutUnknownCode
			}
			failure := message.Failure{
				Category:  message.FailureSubmissionUnknown,
				Code:      code,
				Retryable: false,
			}
			providerResult = ports.ProviderResult{
				Outcome: ports.ProviderOutcomeSubmissionUnknown,
				Failure: &failure,
			}
		} else {
			return DispatchResult{}, fmt.Errorf(
				"%w: provider %q returned an invalid result: %v",
				ErrDispatchInvariant,
				w.provider.Key(),
				err,
			)
		}
	}

	// Once Submit has run, persisting its observation is a bounded critical
	// section. It must still be attempted during consumer shutdown, otherwise a
	// successful provider call would be needlessly left as STARTED/SENDING.
	finalizeCtx, cancelFinalize := context.WithTimeout(
		context.WithoutCancel(ctx),
		w.config.FinalizeTimeout,
	)
	defer cancelFinalize()
	return w.finalize(finalizeCtx, claim, providerResult)
}

type dispatchClaim struct {
	command  DispatchCommand
	request  ports.ProviderRequest
	attempt  ports.StartedDeliveryAttempt
	resolved *DispatchResult
}

func (w *DispatchWorker) claim(
	ctx context.Context,
	command DispatchCommand,
) (dispatchClaim, error) {
	claim := dispatchClaim{command: command}
	var changed *message.Message
	err := w.transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
		record, err := unit.Messages().GetByID(ctx, command.MessageID)
		if errors.Is(err, ports.ErrMessageNotFound) {
			return fmt.Errorf("%w: %s", ErrDispatchMessageMissing, command.MessageID)
		}
		if err != nil {
			return err
		}
		aggregate := record.Message
		if record.TenantID != command.TenantID {
			return fmt.Errorf("%w: dispatch tenant does not own message", ErrDispatchInvariant)
		}

		currentGeneration := aggregate.DispatchGeneration()
		currentSequence := aggregate.LatestSequence()
		if command.DispatchGeneration < currentGeneration || command.AggregateSequence < currentSequence {
			claim.resolved = &DispatchResult{Disposition: DispatchStale}
			return nil
		}
		if command.DispatchGeneration > currentGeneration || command.AggregateSequence > currentSequence {
			return fmt.Errorf(
				"%w: command generation/sequence is ahead of the message",
				ErrDispatchInvariant,
			)
		}
		if aggregate.Status() != message.StatusQueued {
			claim.resolved = &DispatchResult{Disposition: DispatchDuplicate}
			return nil
		}

		now, err := unit.Clock().Now(ctx)
		if err != nil {
			return err
		}
		if !now.Before(aggregate.DispatchDeadline()) {
			changedState, expireErr := aggregate.Expire(now)
			if expireErr != nil || !changedState {
				return fmt.Errorf("%w: queued message could not expire: %v", ErrDispatchInvariant, expireErr)
			}
			if err := saveMessageWithOutbox(ctx, unit, record); err != nil {
				return err
			}
			changed = aggregate
			claim.resolved = &DispatchResult{Disposition: DispatchExpired}
			return nil
		}

		if err := aggregate.StartSending(command.DispatchGeneration, now); err != nil {
			return fmt.Errorf("%w: queued message could not start sending: %v", ErrDispatchInvariant, err)
		}
		attempt := ports.StartedDeliveryAttempt{
			ID:                 uuid.NewString(),
			MessageID:          command.MessageID,
			AttemptNumber:      aggregate.AttemptCount(),
			DispatchGeneration: command.DispatchGeneration,
			ProviderKey:        w.provider.Key(),
			StartedAt:          now,
		}
		if err := attempt.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrDispatchInvariant, err)
		}
		if err := saveMessageWithOutboxAndAttempt(ctx, unit, record, attempt); err != nil {
			return err
		}

		changed = aggregate
		claim.attempt = attempt
		claim.request = ports.ProviderRequest{
			AttemptID:           attempt.ID,
			MessageID:           command.MessageID,
			TenantID:            record.TenantID,
			AttemptNumber:       attempt.AttemptNumber,
			DispatchGeneration:  attempt.DispatchGeneration,
			Category:            record.Category,
			DuplicateRiskPolicy: record.DuplicateRiskPolicy,
		}
		return claim.request.Validate()
	})
	if err != nil {
		return dispatchClaim{}, err
	}
	if changed != nil {
		changed.PullEvents()
	}
	return claim, nil
}

func (w *DispatchWorker) finalize(
	ctx context.Context,
	claim dispatchClaim,
	providerResult ports.ProviderResult,
) (DispatchResult, error) {
	var (
		result  DispatchResult
		changed *message.Message
	)
	err := w.transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
		record, err := unit.Messages().GetByID(ctx, claim.command.MessageID)
		if err != nil {
			return err
		}
		aggregate := record.Message
		if aggregate.DispatchGeneration() != claim.attempt.DispatchGeneration ||
			aggregate.AttemptCount() != claim.attempt.AttemptNumber {
			return fmt.Errorf("%w: attempt fence no longer matches message", ErrDispatchInvariant)
		}
		if aggregate.Status() != message.StatusSending {
			result = DispatchResult{
				Disposition:   DispatchDuplicate,
				AttemptID:     claim.attempt.ID,
				AttemptNumber: claim.attempt.AttemptNumber,
			}
			return nil
		}

		now, err := unit.Clock().Now(ctx)
		if err != nil {
			return err
		}
		completion, disposition, transitionErr := w.applyProviderResult(
			aggregate,
			claim.attempt,
			providerResult,
			now,
		)
		if transitionErr != nil {
			return fmt.Errorf(
				"%w: provider result could not advance message: %v",
				ErrDispatchInvariant,
				transitionErr,
			)
		}
		mapped, err := mapAllMessageEvents(record, aggregate.PendingEvents())
		if err != nil {
			return err
		}
		if _, err := unit.Messages().Save(ctx, aggregate); err != nil {
			return err
		}
		if err := unit.DeliveryAttempts().Complete(ctx, completion); err != nil {
			return err
		}
		if err := unit.DeliveryEvents().Append(ctx, mapped.Delivery); err != nil {
			return err
		}
		if err := unit.Outbox().Append(ctx, mapped.Outbox); err != nil {
			return err
		}
		changed = aggregate
		result = DispatchResult{
			Disposition:   disposition,
			AttemptID:     claim.attempt.ID,
			AttemptNumber: claim.attempt.AttemptNumber,
		}
		return nil
	})
	if err != nil {
		return DispatchResult{}, err
	}
	if changed != nil {
		changed.PullEvents()
	}
	return result, nil
}

func (w *DispatchWorker) applyProviderResult(
	aggregate *message.Message,
	attempt ports.StartedDeliveryAttempt,
	providerResult ports.ProviderResult,
	now time.Time,
) (ports.CompleteDeliveryAttempt, DispatchDisposition, error) {
	completion := ports.CompleteDeliveryAttempt{
		AttemptID:  attempt.ID,
		FinishedAt: now,
	}
	switch providerResult.Outcome {
	case ports.ProviderOutcomeAccepted:
		completion.Status = ports.DeliveryAttemptProviderAccepted
		completion.ProviderMessageID = providerResult.ProviderMessageID
		_, err := aggregate.ApplyDeliveryFact(message.DeliveryFact{
			Kind:              message.FactProviderAccepted,
			OccurredAt:        now,
			ProviderMessageID: providerResult.ProviderMessageID,
		})
		return completion, DispatchProviderAccepted, err
	case ports.ProviderOutcomeSubmissionUnknown:
		completion.Status = ports.DeliveryAttemptSubmissionUnknown
		completion.Failure = providerResult.Failure
		err := aggregate.MarkSubmissionUnknown(*providerResult.Failure, now)
		return completion, DispatchSubmissionUnknown, err
	case ports.ProviderOutcomeFailed:
		completion.Status = ports.DeliveryAttemptFailed
		completion.Failure = providerResult.Failure
		failure := *providerResult.Failure
		if !failure.Retryable {
			err := aggregate.MarkPermanentlyFailed(failure, now)
			return completion, DispatchPermanentlyFailed, err
		}

		if aggregate.AttemptCount() < aggregate.MaxAttempts() {
			delay := w.retry.NextDelay(aggregate.AttemptCount())
			if delay <= 0 || delay > 24*time.Hour {
				return ports.CompleteDeliveryAttempt{}, "", fmt.Errorf(
					"%w: retry policy returned %s",
					ErrInvalidDeliveryRetryDelay,
					delay,
				)
			}
			nextAttemptAt := now.Add(delay)
			if nextAttemptAt.Before(aggregate.DispatchDeadline()) {
				err := aggregate.ScheduleRetry(failure, nextAttemptAt, now)
				return completion, DispatchRetryScheduled, err
			}
		}
		err := aggregate.MarkDeadLettered(failure, now)
		return completion, DispatchDeadLettered, err
	default:
		return ports.CompleteDeliveryAttempt{}, "", fmt.Errorf(
			"%w: unexpected provider outcome %q",
			ErrDispatchInvariant,
			providerResult.Outcome,
		)
	}
}

func saveMessageWithOutbox(
	ctx context.Context,
	unit ports.UnitOfWork,
	record ports.MessageRecord,
) error {
	mapped, err := mapAllMessageEvents(record, record.Message.PendingEvents())
	if err != nil {
		return err
	}
	if _, err := unit.Messages().Save(ctx, record.Message); err != nil {
		return err
	}
	if err := unit.DeliveryEvents().Append(ctx, mapped.Delivery); err != nil {
		return err
	}
	return unit.Outbox().Append(ctx, mapped.Outbox)
}

func saveMessageWithOutboxAndAttempt(
	ctx context.Context,
	unit ports.UnitOfWork,
	record ports.MessageRecord,
	attempt ports.StartedDeliveryAttempt,
) error {
	mapped, err := mapAllMessageEvents(record, record.Message.PendingEvents())
	if err != nil {
		return err
	}
	if _, err := unit.Messages().Save(ctx, record.Message); err != nil {
		return err
	}
	if err := unit.DeliveryAttempts().CreateStarted(ctx, attempt); err != nil {
		return err
	}
	if err := unit.DeliveryEvents().Append(ctx, mapped.Delivery); err != nil {
		return err
	}
	return unit.Outbox().Append(ctx, mapped.Outbox)
}
