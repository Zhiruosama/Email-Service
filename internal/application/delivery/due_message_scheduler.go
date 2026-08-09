package delivery

import (
	"context"
	"errors"
	"fmt"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

var (
	ErrInvalidSchedulerConfig = errors.New("invalid scheduler configuration")
	ErrSchedulerInvariant     = errors.New("scheduler invariant violation")
)

type SchedulerBatchResult struct {
	Claimed uint32
	Queued  uint32
	Expired uint32
}

// DueMessageScheduler advances due database-backed timers in one short,
// bounded transaction. It does not publish to RabbitMQ or call a Provider.
type DueMessageScheduler struct {
	transactor ports.Transactor
	batchSize  uint32
}

func NewDueMessageScheduler(
	transactor ports.Transactor,
	batchSize uint32,
) (*DueMessageScheduler, error) {
	if transactor == nil {
		panic("delivery: nil transactor")
	}
	if batchSize == 0 || batchSize > ports.MaxDueMessageBatchSize {
		return nil, fmt.Errorf(
			"%w: batch size must be in range 1..%d",
			ErrInvalidSchedulerConfig,
			ports.MaxDueMessageBatchSize,
		)
	}
	return &DueMessageScheduler{transactor: transactor, batchSize: batchSize}, nil
}

func (s *DueMessageScheduler) RunOnce(ctx context.Context) (SchedulerBatchResult, error) {
	var (
		committedResult SchedulerBatchResult
		processed       []*message.Message
	)
	err := s.transactor.WithinTransaction(ctx, func(unit ports.UnitOfWork) error {
		batch, err := unit.DueMessages().LockDue(ctx, ports.DueMessageQuery{
			Limit: s.batchSize,
		})
		if err != nil {
			return err
		}
		records := batch.Records
		if len(records) == 0 {
			return nil
		}
		if batch.EvaluatedAt.IsZero() {
			return fmt.Errorf("%w: due batch has no evaluation time", ErrSchedulerInvariant)
		}
		now := batch.EvaluatedAt.UTC()

		candidateResult := SchedulerBatchResult{Claimed: uint32(len(records))}
		outboxEvents := make([]ports.OutboxEvent, 0, len(records)*2)
		deliveryEvents := make([]ports.DeliveryEvent, 0, len(records))
		candidates := make([]*message.Message, 0, len(records))

		for _, record := range records {
			aggregate := record.Message
			if !now.Before(aggregate.DispatchDeadline()) {
				changed, expireErr := aggregate.Expire(now)
				if expireErr != nil {
					return expireErr
				}
				if !changed {
					return fmt.Errorf(
						"%w: due message %s was already expired",
						ErrSchedulerInvariant,
						aggregate.ID(),
					)
				}
				candidateResult.Expired++
			} else {
				if queueErr := aggregate.Queue(now); queueErr != nil {
					return queueErr
				}
				candidateResult.Queued++
			}

			mapped, mapErr := mapAllMessageEvents(record, aggregate.PendingEvents())
			if mapErr != nil {
				return mapErr
			}
			if _, saveErr := unit.Messages().Save(ctx, aggregate); saveErr != nil {
				return saveErr
			}
			outboxEvents = append(outboxEvents, mapped.Outbox...)
			deliveryEvents = append(deliveryEvents, mapped.Delivery...)
			candidates = append(candidates, aggregate)
		}

		if err := unit.DeliveryEvents().Append(ctx, deliveryEvents); err != nil {
			return err
		}
		if err := unit.Outbox().Append(ctx, outboxEvents); err != nil {
			return err
		}
		committedResult = candidateResult
		processed = candidates
		return nil
	})
	if err != nil {
		return SchedulerBatchResult{}, err
	}
	for _, aggregate := range processed {
		aggregate.PullEvents()
	}
	return committedResult, nil
}
