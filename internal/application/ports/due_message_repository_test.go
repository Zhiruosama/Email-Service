package ports_test

import (
	"errors"
	"testing"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

func TestDueMessageQueryValidate(t *testing.T) {
	t.Parallel()

	if err := (ports.DueMessageQuery{Limit: 100}).Validate(); err != nil {
		t.Fatalf("valid query: %v", err)
	}

	tests := []struct {
		name  string
		query ports.DueMessageQuery
	}{
		{name: "zero limit", query: ports.DueMessageQuery{}},
		{
			name: "oversized limit",
			query: ports.DueMessageQuery{
				Limit: ports.MaxDueMessageBatchSize + 1,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.query.Validate(); !errors.Is(err, ports.ErrInvalidDueMessageQuery) {
				t.Fatalf("Validate() error = %v, want ErrInvalidDueMessageQuery", err)
			}
		})
	}
}
