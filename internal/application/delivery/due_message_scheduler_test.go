package delivery

import (
	"context"
	"errors"
	"testing"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

func TestNewDueMessageSchedulerValidatesConfiguration(t *testing.T) {
	t.Parallel()

	transactor := noCallTransactor{}
	for _, batchSize := range []uint32{0, ports.MaxDueMessageBatchSize + 1} {
		if _, err := NewDueMessageScheduler(transactor, batchSize); !errors.Is(err, ErrInvalidSchedulerConfig) {
			t.Fatalf("batch size %d error = %v, want ErrInvalidSchedulerConfig", batchSize, err)
		}
	}
	if _, err := NewDueMessageScheduler(transactor, 1); err != nil {
		t.Fatalf("valid scheduler: %v", err)
	}
}

func TestNewDueMessageSchedulerRejectsNilTransactor(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("NewDueMessageScheduler(nil) did not panic")
		}
	}()
	_, _ = NewDueMessageScheduler(nil, 1)
}

type noCallTransactor struct{}

func (noCallTransactor) WithinTransaction(context.Context, ports.TransactionFunc) error {
	panic("transaction must not be called")
}
