package grpcapi

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	deliveryv1 "github.com/Zhiruosama/Email-Service/gen/go/mailservice/delivery/v1"
	deliveryapp "github.com/Zhiruosama/Email-Service/internal/application/delivery"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const grpcTestTenant = "b0000000-0000-4000-8000-000000000001"

func TestDeliveryServerRequiresAuthenticatedTenant(t *testing.T) {
	server := NewDeliveryServer(stubSubmitter{}, stubQuerier{})
	_, err := server.SubmitEmail(context.Background(), validProtoSubmitRequest(t))
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("status = %s, want Unauthenticated: %v", status.Code(err), err)
	}
}

func TestSubmitEmailMapsProtoAndReturnsOnlySafeView(t *testing.T) {
	record := grpcTestRecord(t)
	submitter := &capturingSubmitter{result: deliveryapp.SubmitEmailResult{
		Disposition: deliveryapp.SubmitEmailAccepted,
		Record:      record,
	}}
	server := NewDeliveryServer(submitter, stubQuerier{})
	ctx := context.WithValue(context.Background(), tenantContextKey{}, grpcTestTenant)
	response, err := server.SubmitEmail(ctx, validProtoSubmitRequest(t))
	if err != nil {
		t.Fatalf("submit email: %v", err)
	}
	if submitter.command.TenantID != grpcTestTenant ||
		submitter.command.RecipientEmail != "user@example.com" ||
		submitter.command.TemplateKey != "verification_code.v1" ||
		string(submitter.command.Variables) == "" {
		t.Fatalf("unexpected application command: %#v", submitter.command)
	}
	if response.Disposition != deliveryv1.SubmitDisposition_SUBMIT_DISPOSITION_ACCEPTED ||
		response.Message.MessageId != record.Message.ID() ||
		response.Message.Recipient.MaskedEmail != "u***@example.com" ||
		response.Message.Template.GetVersion() != 1 {
		t.Fatalf("unexpected safe response: %#v", response)
	}
	encoded, _ := json.Marshal(response)
	if string(encoded) == "" || containsAny(string(encoded), "123456", "user@example.com", "encrypted") {
		t.Fatalf("response leaked sensitive submission data: %s", encoded)
	}
}

func TestSubmitEmailRejectsUint32PriorityBeforeNarrowing(t *testing.T) {
	server := NewDeliveryServer(stubSubmitter{}, stubQuerier{})
	ctx := context.WithValue(context.Background(), tenantContextKey{}, grpcTestTenant)
	request := validProtoSubmitRequest(t)
	request.Priority = 265
	_, err := server.SubmitEmail(ctx, request)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status = %s, want InvalidArgument: %v", status.Code(err), err)
	}
}

func TestGetEmailMapsSelectorAndRepositoryNotFound(t *testing.T) {
	querier := &capturingQuerier{err: ports.ErrMessageNotFound}
	server := NewDeliveryServer(stubSubmitter{}, querier)
	ctx := context.WithValue(context.Background(), tenantContextKey{}, grpcTestTenant)
	_, err := server.GetEmail(ctx, &deliveryv1.GetEmailRequest{
		Selector: &deliveryv1.GetEmailRequest_IdempotencyKey{IdempotencyKey: "request-1"},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("status = %s, want NotFound: %v", status.Code(err), err)
	}
	if querier.query.TenantID != grpcTestTenant || querier.query.IdempotencyKey != "request-1" {
		t.Fatalf("unexpected query: %#v", querier.query)
	}
}

func TestPublicErrorUsesStableSanitizedCodes(t *testing.T) {
	tests := []struct {
		err  error
		code codes.Code
	}{
		{deliveryapp.ErrInvalidSubmission, codes.InvalidArgument},
		{ports.ErrTemplateNotAllowed, codes.PermissionDenied},
		{ports.ErrTemplateNotFound, codes.NotFound},
		{ports.ErrIdempotencyConflict, codes.AlreadyExists},
		{ports.ErrTransaction, codes.Unavailable},
		{deliveryapp.ErrSubmissionInvariant, codes.Internal},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
	}
	for _, test := range tests {
		mapped := publicError(test.err)
		if status.Code(mapped) != test.code {
			t.Errorf("error %v mapped to %s, want %s", test.err, status.Code(mapped), test.code)
		}
		if errors.Is(mapped, test.err) {
			t.Errorf("public error exposed internal error chain for %v", test.err)
		}
	}
}

func validProtoSubmitRequest(t *testing.T) *deliveryv1.SubmitEmailRequest {
	t.Helper()
	variables, err := structpb.NewStruct(map[string]any{
		"code":              "123456",
		"purpose":           "LOGIN",
		"valid_for_seconds": 300,
	})
	if err != nil {
		t.Fatalf("create variables: %v", err)
	}
	return &deliveryv1.SubmitEmailRequest{
		IdempotencyKey:    "request-1",
		Recipient:         &deliveryv1.Recipient{Email: "user@example.com"},
		SenderIdentityKey: "ainexus.default",
		Content: &deliveryv1.EmailContent{
			Template:  &deliveryv1.TemplateReference{Key: "verification_code.v1"},
			Locale:    "zh-CN",
			Variables: variables,
		},
		Category:            deliveryv1.EmailCategory_EMAIL_CATEGORY_CRITICAL,
		Priority:            9,
		DispatchDeadline:    timestamppb.New(time.Now().UTC().Add(2 * time.Minute)),
		DuplicateRiskPolicy: deliveryv1.DuplicateRiskPolicy_DUPLICATE_RISK_POLICY_AVOID_DUPLICATE,
	}
}

func grpcTestRecord(t *testing.T) ports.MessageRecord {
	t.Helper()
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	aggregate, err := message.New(message.NewParams{
		ID:               "b1000000-0000-4000-8000-000000000001",
		Now:              now,
		DispatchDeadline: now.Add(2 * time.Minute),
		MaxAttempts:      3,
	})
	if err != nil {
		t.Fatalf("create message: %v", err)
	}
	aggregate.PullEvents()
	return ports.MessageRecord{
		TenantID:            grpcTestTenant,
		IdempotencyKey:      "request-1",
		PayloadFingerprint:  [32]byte{1},
		Category:            ports.EmailCategoryCritical,
		Priority:            9,
		DuplicateRiskPolicy: ports.DuplicateRiskAvoidDuplicate,
		Submission: &ports.SubmissionDetails{
			SenderIdentityKey: "ainexus.default",
			TemplateKey:       "verification_code.v1",
			TemplateVersion:   1,
			Locale:            "zh-CN",
			RecipientMasked:   "u***@example.com",
			PayloadKeyID:      "key-1",
			EncryptedPayload:  make([]byte, 29),
			Metadata:          json.RawMessage(`{}`),
		},
		Message: aggregate,
	}
}

type capturingSubmitter struct {
	command deliveryapp.SubmitEmailCommand
	result  deliveryapp.SubmitEmailResult
	err     error
}

func (s *capturingSubmitter) Submit(_ context.Context, command deliveryapp.SubmitEmailCommand) (deliveryapp.SubmitEmailResult, error) {
	s.command = command
	return s.result, s.err
}

type stubSubmitter struct{}

func (stubSubmitter) Submit(context.Context, deliveryapp.SubmitEmailCommand) (deliveryapp.SubmitEmailResult, error) {
	panic("must not be called")
}

type capturingQuerier struct {
	query  deliveryapp.GetEmailQuery
	record ports.MessageRecord
	err    error
}

func (q *capturingQuerier) Get(_ context.Context, query deliveryapp.GetEmailQuery) (ports.MessageRecord, error) {
	q.query = query
	return q.record, q.err
}

type stubQuerier struct{}

func (stubQuerier) Get(context.Context, deliveryapp.GetEmailQuery) (ports.MessageRecord, error) {
	panic("must not be called")
}

func containsAny(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if strings.Contains(value, candidate) {
			return true
		}
	}
	return false
}
