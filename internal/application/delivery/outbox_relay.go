package delivery

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/google/uuid"
)

var (
	ErrInvalidOutboxRelayConfig = errors.New("invalid outbox relay configuration")
	ErrInvalidOutboxRetryDelay  = errors.New("invalid outbox retry delay")
	ErrOutboxRelayInvariant     = errors.New("outbox relay invariant violation")
)

const (
	publishTimeoutCode  = "PUBLISH_TIMEOUT"
	publishCanceledCode = "PUBLISH_CANCELED"
	publishInternalCode = "PUBLISH_INTERNAL"
)

type OutboxRelayConfig struct {
	InstanceID         string
	BatchSize          uint32
	PublishConcurrency uint32
	LeaseDuration      time.Duration
	PublishTimeout     time.Duration
	MaxAttempts        uint32
}

func (c OutboxRelayConfig) Validate() error {
	instanceID := strings.TrimSpace(c.InstanceID)
	if instanceID == "" || instanceID != c.InstanceID || len(c.InstanceID) > 200 ||
		strings.ContainsAny(c.InstanceID, "\r\n/") {
		return fmt.Errorf(
			"%w: instance id must contain 1..200 bytes without whitespace, newlines, or slash",
			ErrInvalidOutboxRelayConfig,
		)
	}
	if c.BatchSize == 0 || c.BatchSize > ports.MaxOutboxDeliveryBatchSize {
		return fmt.Errorf(
			"%w: batch size must be in range 1..%d",
			ErrInvalidOutboxRelayConfig,
			ports.MaxOutboxDeliveryBatchSize,
		)
	}
	if c.PublishConcurrency == 0 || c.PublishConcurrency > c.BatchSize {
		return fmt.Errorf(
			"%w: publish concurrency must be in range 1..batch size",
			ErrInvalidOutboxRelayConfig,
		)
	}
	if c.LeaseDuration < time.Second || c.LeaseDuration > time.Hour {
		return fmt.Errorf(
			"%w: lease duration must be in range 1s..1h",
			ErrInvalidOutboxRelayConfig,
		)
	}
	if c.PublishTimeout <= 0 || c.PublishTimeout >= c.LeaseDuration {
		return fmt.Errorf(
			"%w: publish timeout must be positive and shorter than lease duration",
			ErrInvalidOutboxRelayConfig,
		)
	}
	if c.MaxAttempts == 0 || c.MaxAttempts > math.MaxInt32 {
		return fmt.Errorf(
			"%w: max attempts must fit a positive PostgreSQL INTEGER",
			ErrInvalidOutboxRelayConfig,
		)
	}
	return nil
}

type OutboxRelayResult struct {
	Claimed      uint32
	Published    uint32
	Retried      uint32
	DeadLettered uint32
	LeaseLost    uint32
}

type OutboxRelay struct {
	transactor ports.Transactor
	publisher  ports.OutboxPublisher
	retry      OutboxRetryPolicy
	config     OutboxRelayConfig
}

func NewOutboxRelay(
	transactor ports.Transactor,
	publisher ports.OutboxPublisher,
	retryPolicy OutboxRetryPolicy,
	config OutboxRelayConfig,
) (*OutboxRelay, error) {
	if transactor == nil {
		panic("delivery: nil transactor")
	}
	if publisher == nil {
		panic("delivery: nil outbox publisher")
	}
	if retryPolicy == nil {
		panic("delivery: nil outbox retry policy")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &OutboxRelay{
		transactor: transactor,
		publisher:  publisher,
		retry:      retryPolicy,
		config:     config,
	}, nil
}

func (r *OutboxRelay) RunOnce(ctx context.Context) (OutboxRelayResult, error) {
	leaseToken := r.config.InstanceID + "/" + uuid.NewString()
	var batch ports.OutboxClaimBatch
	err := r.transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
		claimed, claimErr := unit.OutboxDeliveries().ClaimPending(ctx, ports.OutboxClaimQuery{
			LeaseToken:    leaseToken,
			LeaseDuration: r.config.LeaseDuration,
			Limit:         r.config.BatchSize,
		})
		if claimErr != nil {
			return claimErr
		}
		if validateErr := validateClaimedOutboxBatch(claimed, leaseToken); validateErr != nil {
			return validateErr
		}
		batch = claimed
		return nil
	})
	if err != nil {
		return OutboxRelayResult{}, err
	}
	if len(batch.Events) == 0 {
		return OutboxRelayResult{}, nil
	}

	result := OutboxRelayResult{Claimed: uint32(len(batch.Events))}
	jobs := make(chan ports.LeasedOutboxEvent)
	outcomes := make(chan relayEventOutcome, len(batch.Events))
	workerCount := int(r.config.PublishConcurrency)
	if workerCount > len(batch.Events) {
		workerCount = len(batch.Events)
	}

	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for leased := range jobs {
				outcomes <- r.publishOne(ctx, leased)
			}
		}()
	}
	go func() {
		for _, leased := range batch.Events {
			jobs <- leased
		}
		close(jobs)
		workers.Wait()
		close(outcomes)
	}()

	var outcomeErrors []error
	for outcome := range outcomes {
		switch outcome.kind {
		case relayOutcomePublished:
			result.Published++
		case relayOutcomeRetried:
			result.Retried++
		case relayOutcomeDeadLettered:
			result.DeadLettered++
		case relayOutcomeLeaseLost:
			result.LeaseLost++
		}
		if outcome.err != nil {
			outcomeErrors = append(outcomeErrors, outcome.err)
		}
	}
	return result, errors.Join(outcomeErrors...)
}

func validateClaimedOutboxBatch(batch ports.OutboxClaimBatch, leaseToken string) error {
	if len(batch.Events) == 0 {
		return nil
	}
	if batch.EvaluatedAt.IsZero() {
		return fmt.Errorf("%w: claim batch has no database timestamp", ErrOutboxRelayInvariant)
	}
	for _, leased := range batch.Events {
		if err := leased.Validate(); err != nil {
			return fmt.Errorf("%w: invalid leased event: %v", ErrOutboxRelayInvariant, err)
		}
		if leased.LeaseToken != leaseToken {
			return fmt.Errorf("%w: claim token does not match batch", ErrOutboxRelayInvariant)
		}
		if !leased.LeaseUntil.After(batch.EvaluatedAt) {
			return fmt.Errorf("%w: lease is not after claim time", ErrOutboxRelayInvariant)
		}
	}
	return nil
}

func (r *OutboxRelay) publishOne(
	ctx context.Context,
	leased ports.LeasedOutboxEvent,
) relayEventOutcome {
	if err := ctx.Err(); err != nil {
		return relayEventOutcome{err: err}
	}

	publishCtx, cancel := context.WithTimeout(ctx, r.config.PublishTimeout)
	publishErr := r.publisher.Publish(publishCtx, ports.OutboxPublication{
		Event:         leased.Event,
		AttemptNumber: leased.AttemptNumber,
	})
	publishContextErr := publishCtx.Err()
	cancel()

	if err := ctx.Err(); err != nil {
		// The claim remains fenced until expiry. Shutdown should not start a new
		// database transaction with an already-canceled parent context.
		return relayEventOutcome{err: err}
	}
	reference := ports.OutboxLeaseReference{
		EventID:       leased.Event.ID,
		LeaseToken:    leased.LeaseToken,
		AttemptNumber: leased.AttemptNumber,
	}
	if publishErr == nil {
		return r.completePublished(ctx, reference)
	}

	failure := classifyPublishFailure(publishErr, publishContextErr)
	if !failure.retryable || leased.AttemptNumber >= r.config.MaxAttempts {
		return r.completeDeadLetter(ctx, reference, failure.code)
	}
	delay := r.retry.NextDelay(leased.AttemptNumber)
	if delay < 0 || delay > 24*time.Hour {
		return relayEventOutcome{
			err: fmt.Errorf(
				"%w: retry policy returned %s",
				ErrInvalidOutboxRetryDelay,
				delay,
			),
		}
	}
	return r.completeReschedule(ctx, reference, delay, failure.code)
}

func (r *OutboxRelay) completePublished(
	ctx context.Context,
	reference ports.OutboxLeaseReference,
) relayEventOutcome {
	err := r.transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
		return unit.OutboxDeliveries().MarkPublished(ctx, reference)
	})
	return completionOutcome(relayOutcomePublished, err)
}

func (r *OutboxRelay) completeReschedule(
	ctx context.Context,
	reference ports.OutboxLeaseReference,
	delay time.Duration,
	errorCode string,
) relayEventOutcome {
	err := r.transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
		return unit.OutboxDeliveries().Reschedule(ctx, ports.RescheduleOutboxCommand{
			Lease:     reference,
			Delay:     delay,
			ErrorCode: errorCode,
		})
	})
	return completionOutcome(relayOutcomeRetried, err)
}

func (r *OutboxRelay) completeDeadLetter(
	ctx context.Context,
	reference ports.OutboxLeaseReference,
	errorCode string,
) relayEventOutcome {
	err := r.transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
		return unit.OutboxDeliveries().DeadLetter(ctx, ports.DeadLetterOutboxCommand{
			Lease:     reference,
			ErrorCode: errorCode,
		})
	})
	return completionOutcome(relayOutcomeDeadLettered, err)
}

type relayOutcomeKind uint8

const (
	relayOutcomeNone relayOutcomeKind = iota
	relayOutcomePublished
	relayOutcomeRetried
	relayOutcomeDeadLettered
	relayOutcomeLeaseLost
)

type relayEventOutcome struct {
	kind relayOutcomeKind
	err  error
}

func completionOutcome(success relayOutcomeKind, err error) relayEventOutcome {
	if errors.Is(err, ports.ErrOutboxLeaseLost) {
		return relayEventOutcome{kind: relayOutcomeLeaseLost}
	}
	if err != nil {
		return relayEventOutcome{err: err}
	}
	return relayEventOutcome{kind: success}
}

type classifiedPublishFailure struct {
	code      string
	retryable bool
}

func classifyPublishFailure(
	publishErr error,
	publishContextErr error,
) classifiedPublishFailure {
	if errors.Is(publishContextErr, context.DeadlineExceeded) {
		return classifiedPublishFailure{code: publishTimeoutCode, retryable: true}
	}
	if errors.Is(publishContextErr, context.Canceled) {
		return classifiedPublishFailure{code: publishCanceledCode, retryable: true}
	}
	var typed *ports.OutboxPublishError
	if errors.As(publishErr, &typed) && typed.Validate() == nil {
		return classifiedPublishFailure{code: typed.Code, retryable: typed.Retryable}
	}
	return classifiedPublishFailure{code: publishInternalCode, retryable: true}
}
