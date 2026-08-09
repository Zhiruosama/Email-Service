package delivery

import (
	"context"
	"errors"
	"testing"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

func TestEmailQueryRejectsInvalidSelectorsBeforeRepository(t *testing.T) {
	service := NewEmailQueryService(noCallMessageRepository{})
	tests := []GetEmailQuery{
		{},
		{TenantID: "tenant", MessageID: "90000000-0000-4000-8000-000000000001"},
		{TenantID: "90000000-0000-4000-8000-000000000002"},
		{
			TenantID:       "90000000-0000-4000-8000-000000000002",
			MessageID:      "90000000-0000-4000-8000-000000000001",
			IdempotencyKey: "request-1",
		},
		{TenantID: "90000000-0000-4000-8000-000000000002", MessageID: "not-a-uuid"},
		{TenantID: "90000000-0000-4000-8000-000000000002", IdempotencyKey: "bad key"},
	}
	for index, query := range tests {
		if _, err := service.Get(context.Background(), query); !errors.Is(err, ErrInvalidEmailQuery) {
			t.Errorf("case %d error = %v, want ErrInvalidEmailQuery", index, err)
		}
	}
}

func TestNewEmailQueryServiceRejectsNilRepository(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("constructor did not panic")
		}
	}()
	NewEmailQueryService(nil)
}

func TestEmailQueryHidesCrossTenantMessageID(t *testing.T) {
	record := deliveryTestRecord(t, "90000000-0000-4000-8000-000000000001")
	record.TenantID = "90000000-0000-4000-8000-000000000003"
	service := NewEmailQueryService(fixedMessageRepository{record: record})
	_, err := service.Get(context.Background(), GetEmailQuery{
		TenantID:  "90000000-0000-4000-8000-000000000002",
		MessageID: record.Message.ID(),
	})
	if !errors.Is(err, ports.ErrMessageNotFound) {
		t.Fatalf("cross-tenant error = %v, want ErrMessageNotFound", err)
	}
}

type noCallMessageRepository struct{}

func (noCallMessageRepository) Create(context.Context, ports.MessageRecord) (ports.CreateMessageResult, error) {
	panic("must not be called")
}
func (noCallMessageRepository) GetByID(context.Context, string) (ports.MessageRecord, error) {
	panic("must not be called")
}
func (noCallMessageRepository) GetByIdempotencyKey(context.Context, string, string) (ports.MessageRecord, error) {
	panic("must not be called")
}
func (noCallMessageRepository) Save(context.Context, *message.Message) (uint64, error) {
	panic("must not be called")
}

type fixedMessageRepository struct {
	record ports.MessageRecord
}

func (fixedMessageRepository) Create(context.Context, ports.MessageRecord) (ports.CreateMessageResult, error) {
	panic("must not be called")
}
func (r fixedMessageRepository) GetByID(context.Context, string) (ports.MessageRecord, error) {
	return r.record, nil
}
func (fixedMessageRepository) GetByIdempotencyKey(context.Context, string, string) (ports.MessageRecord, error) {
	panic("must not be called")
}
func (fixedMessageRepository) Save(context.Context, *message.Message) (uint64, error) {
	panic("must not be called")
}
