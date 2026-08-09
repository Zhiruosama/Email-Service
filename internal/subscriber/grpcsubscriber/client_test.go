package grpcsubscriber

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	deliveryv1 "github.com/Zhiruosama/Email-Service/gen/go/mailservice/delivery/v1"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestClientReportsCompleteSanitizedEventToRealGRPCServer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	receiver := &recordingReceiver{disposition: deliveryv1.EventAckDisposition_EVENT_ACK_DISPOSITION_ACCEPTED}
	deliveryv1.RegisterDeliveryEventReceiverServiceServer(server, receiver)
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-serveErrors
	})

	client, err := Dial(Config{Address: listener.Addr().String()}, insecure.NewCredentials())
	if err != nil {
		t.Fatalf("dial callback: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	event := grpcTestEvent()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	disposition, err := client.Report(ctx, event)
	if err != nil {
		t.Fatalf("report event: %v", err)
	}
	if disposition != ports.EventAckAccepted {
		t.Fatalf("disposition = %q, want ACCEPTED", disposition)
	}
	request := receiver.request
	if request == nil || request.Event == nil {
		t.Fatal("server received no event")
	}
	got := request.Event
	if got.EventId != event.ID || got.MessageId != event.MessageID ||
		got.IdempotencyKey != event.IdempotencyKey || got.Sequence != event.Sequence ||
		got.AttemptNumber != event.AttemptNumber ||
		got.GetProviderMessageId() != event.ProviderMessageID ||
		got.Status != deliveryv1.DeliveryStatus_DELIVERY_STATUS_PERMANENTLY_FAILED ||
		got.Failure == nil || got.Failure.Code != event.Failure.Code ||
		!got.OccurredAt.AsTime().Equal(event.OccurredAt) ||
		!got.ObservedAt.AsTime().Equal(event.ObservedAt) {
		t.Fatalf("unexpected callback event: %#v", got)
	}
}

func TestClientMapsAllSuccessDispositions(t *testing.T) {
	t.Parallel()
	event := grpcTestEvent()
	tests := []struct {
		proto deliveryv1.EventAckDisposition
		want  ports.EventAckDisposition
	}{
		{deliveryv1.EventAckDisposition_EVENT_ACK_DISPOSITION_ACCEPTED, ports.EventAckAccepted},
		{deliveryv1.EventAckDisposition_EVENT_ACK_DISPOSITION_DUPLICATE, ports.EventAckDuplicate},
		{deliveryv1.EventAckDisposition_EVENT_ACK_DISPOSITION_IGNORED_STALE, ports.EventAckIgnoredStale},
	}
	for _, test := range tests {
		client := New(fakeReceiverClient{response: &deliveryv1.ReportDeliveryEventResponse{
			EventId: event.ID, Disposition: test.proto,
		}})
		got, err := client.Report(context.Background(), event)
		if err != nil || got != test.want {
			t.Errorf("proto %s result = %q/%v, want %q", test.proto, got, err, test.want)
		}
	}
}

func TestClientRejectsInvalidResponseAsPermanentProtocolFailure(t *testing.T) {
	t.Parallel()
	event := grpcTestEvent()
	for _, response := range []*deliveryv1.ReportDeliveryEventResponse{
		nil,
		{EventId: "another-event", Disposition: deliveryv1.EventAckDisposition_EVENT_ACK_DISPOSITION_ACCEPTED},
		{EventId: event.ID, Disposition: deliveryv1.EventAckDisposition_EVENT_ACK_DISPOSITION_UNSPECIFIED},
	} {
		client := New(fakeReceiverClient{response: response})
		_, err := client.Report(context.Background(), event)
		assertSubscriberError(t, err, "GRPC_PROTOCOL_ERROR", false)
	}
}

func TestClientClassifiesGRPCStatusErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code      codes.Code
		stable    string
		retryable bool
	}{
		{codes.Unavailable, "GRPC_UNAVAILABLE", true},
		{codes.ResourceExhausted, "GRPC_RESOURCE_EXHAUSTED", true},
		{codes.DeadlineExceeded, "GRPC_DEADLINE_EXCEEDED", true},
		{codes.NotFound, "GRPC_NOT_FOUND", true},
		{codes.InvalidArgument, "GRPC_INVALID_ARGUMENT", false},
		{codes.Unauthenticated, "GRPC_UNAUTHENTICATED", false},
		{codes.PermissionDenied, "GRPC_PERMISSION_DENIED", false},
		{codes.Unimplemented, "GRPC_UNIMPLEMENTED", false},
	}
	for _, test := range tests {
		t.Run(test.code.String(), func(t *testing.T) {
			t.Parallel()
			client := New(fakeReceiverClient{err: status.Error(test.code, "private detail")})
			_, err := client.Report(context.Background(), grpcTestEvent())
			assertSubscriberError(t, err, test.stable, test.retryable)
		})
	}
}

func TestClientRejectsInvalidEventBeforeRPC(t *testing.T) {
	t.Parallel()
	event := grpcTestEvent()
	event.ObservedAt = time.Time{}
	client := New(noCallReceiverClient{})
	_, err := client.Report(context.Background(), event)
	assertSubscriberError(t, err, "CALLBACK_EVENT_INVALID", false)
}

func TestStatusAndFailureMappingsCoverDomainValues(t *testing.T) {
	t.Parallel()
	statuses := []message.Status{
		message.StatusAccepted, message.StatusScheduled, message.StatusQueued,
		message.StatusSending, message.StatusRetryScheduled, message.StatusSubmissionUnknown,
		message.StatusProviderAccepted, message.StatusDelivered, message.StatusBounced,
		message.StatusComplained, message.StatusCanceled, message.StatusExpired,
		message.StatusPermanentlyFailed, message.StatusDeadLettered, message.StatusUnknownFinal,
	}
	for _, value := range statuses {
		mapped, ok := statusToProto(value)
		if !ok || mapped == deliveryv1.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED {
			t.Errorf("status %q was not mapped", value)
		}
	}
	categories := []message.FailureCategory{
		message.FailureValidation, message.FailureAuthentication, message.FailureRateLimited,
		message.FailureRecipientRejected, message.FailureContentRejected, message.FailureProviderDown,
		message.FailureNetwork, message.FailureTimeoutBeforeSend, message.FailureSubmissionUnknown,
		message.FailureInternal,
	}
	for _, category := range categories {
		mapped := failureToProto(message.Failure{Category: category, Code: "CODE"})
		if mapped.Category == deliveryv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
			t.Errorf("failure category %q was not mapped", category)
		}
	}
}

func TestConstructorsRequireDependencies(t *testing.T) {
	t.Parallel()
	for _, construct := range []func(){
		func() { New(nil) },
		func() { _, _ = Dial(Config{Address: "localhost:1234"}, nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("constructor did not panic")
				}
			}()
			construct()
		}()
	}
}

func grpcTestEvent() ports.PersistedDeliveryEvent {
	occurredAt := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	failure := &message.Failure{
		Category:  message.FailureInternal,
		Code:      "PROVIDER_REJECTED",
		Retryable: false,
	}
	return ports.PersistedDeliveryEvent{
		DeliveryEvent: ports.DeliveryEvent{
			ID:                "e0000000-0000-4000-8000-000000000001",
			TenantID:          "e0000000-0000-4000-8000-000000000002",
			MessageID:         "e0000000-0000-4000-8000-000000000003",
			IdempotencyKey:    "request-1",
			Status:            message.StatusPermanentlyFailed,
			Sequence:          4,
			AttemptNumber:     1,
			ProviderMessageID: "provider-1",
			Failure:           failure,
			OccurredAt:        occurredAt,
		},
		ObservedAt: occurredAt.Add(time.Second),
	}
}

type fakeReceiverClient struct {
	response *deliveryv1.ReportDeliveryEventResponse
	err      error
}

func (c fakeReceiverClient) ReportDeliveryEvent(
	context.Context,
	*deliveryv1.ReportDeliveryEventRequest,
	...grpc.CallOption,
) (*deliveryv1.ReportDeliveryEventResponse, error) {
	return c.response, c.err
}

type noCallReceiverClient struct{}

func (noCallReceiverClient) ReportDeliveryEvent(
	context.Context,
	*deliveryv1.ReportDeliveryEventRequest,
	...grpc.CallOption,
) (*deliveryv1.ReportDeliveryEventResponse, error) {
	panic("must not be called")
}

type recordingReceiver struct {
	deliveryv1.UnimplementedDeliveryEventReceiverServiceServer
	disposition deliveryv1.EventAckDisposition
	request     *deliveryv1.ReportDeliveryEventRequest
}

func (r *recordingReceiver) ReportDeliveryEvent(
	_ context.Context,
	request *deliveryv1.ReportDeliveryEventRequest,
) (*deliveryv1.ReportDeliveryEventResponse, error) {
	r.request = request
	return &deliveryv1.ReportDeliveryEventResponse{
		EventId:     request.Event.EventId,
		Disposition: r.disposition,
	}, nil
}

func assertSubscriberError(t *testing.T, err error, code string, retryable bool) {
	t.Helper()
	var subscriberErr *ports.DeliveryEventSubscriberError
	if !errors.As(err, &subscriberErr) || subscriberErr.Code != code || subscriberErr.Retryable != retryable {
		t.Fatalf("subscriber error = %#v, want code=%s retryable=%t", err, code, retryable)
	}
}
