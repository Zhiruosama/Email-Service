// Package grpcsubscriber implements the outbound delivery-event callback over
// the public mailservice.delivery.v1 gRPC contract.
package grpcsubscriber

import (
	"context"
	"fmt"
	"strings"

	deliveryv1 "github.com/Zhiruosama/Email-Service/gen/go/mailservice/delivery/v1"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Client struct {
	client deliveryv1.DeliveryEventReceiverServiceClient
	close  func() error
}

var _ ports.DeliveryEventSubscriber = (*Client)(nil)

func New(client deliveryv1.DeliveryEventReceiverServiceClient) *Client {
	if client == nil {
		panic("grpc subscriber: nil delivery event receiver client")
	}
	return &Client{client: client}
}

// Dial requires explicit transport credentials. Development callers may pass
// insecure.NewCredentials(); production composition can pass TLS or mTLS
// credentials without changing the callback adapter.
func Dial(config Config, transportCredentials credentials.TransportCredentials) (*Client, error) {
	if transportCredentials == nil {
		panic("grpc subscriber: nil transport credentials")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	connection, err := grpc.NewClient(
		config.Address,
		grpc.WithTransportCredentials(transportCredentials),
	)
	if err != nil {
		return nil, fmt.Errorf("create gRPC subscriber connection: %w", err)
	}
	client := New(deliveryv1.NewDeliveryEventReceiverServiceClient(connection))
	client.close = connection.Close
	return client, nil
}

func (c *Client) Close() error {
	if c == nil || c.close == nil {
		return nil
	}
	return c.close()
}

func (c *Client) Report(
	ctx context.Context,
	event ports.PersistedDeliveryEvent,
) (ports.EventAckDisposition, error) {
	request, err := reportRequest(event)
	if err != nil {
		return "", ports.NewDeliveryEventSubscriberError(
			"CALLBACK_EVENT_INVALID",
			false,
			err,
		)
	}
	response, err := c.client.ReportDeliveryEvent(ctx, request)
	if err != nil {
		code, retryable := classifyGRPCError(err)
		return "", ports.NewDeliveryEventSubscriberError(code, retryable, err)
	}
	if response == nil || response.EventId != event.ID {
		return "", ports.NewDeliveryEventSubscriberError(
			"GRPC_PROTOCOL_ERROR",
			false,
			fmt.Errorf("callback response event id does not match request"),
		)
	}
	disposition, ok := dispositionFromProto(response.Disposition)
	if !ok {
		return "", ports.NewDeliveryEventSubscriberError(
			"GRPC_PROTOCOL_ERROR",
			false,
			fmt.Errorf("callback response has an unknown disposition"),
		)
	}
	return disposition, nil
}

func reportRequest(event ports.PersistedDeliveryEvent) (*deliveryv1.ReportDeliveryEventRequest, error) {
	if err := event.Validate(); err != nil {
		return nil, err
	}
	occurredAt := timestamppb.New(event.OccurredAt)
	observedAt := timestamppb.New(event.ObservedAt)
	if err := occurredAt.CheckValid(); err != nil {
		return nil, fmt.Errorf("occurred_at is outside the protobuf timestamp range: %w", err)
	}
	if err := observedAt.CheckValid(); err != nil {
		return nil, fmt.Errorf("observed_at is outside the protobuf timestamp range: %w", err)
	}
	statusValue, ok := statusToProto(event.Status)
	if !ok {
		return nil, fmt.Errorf("unknown delivery status %q", event.Status)
	}
	callbackEvent := &deliveryv1.DeliveryEvent{
		EventId:        event.ID,
		MessageId:      event.MessageID,
		IdempotencyKey: event.IdempotencyKey,
		Status:         statusValue,
		OccurredAt:     occurredAt,
		ObservedAt:     observedAt,
		Sequence:       event.Sequence,
		AttemptNumber:  event.AttemptNumber,
	}
	if event.ProviderMessageID != "" {
		callbackEvent.ProviderMessageId = stringPointer(event.ProviderMessageID)
	}
	if event.Failure != nil {
		callbackEvent.Failure = failureToProto(*event.Failure)
	}
	return &deliveryv1.ReportDeliveryEventRequest{Event: callbackEvent}, nil
}

func statusToProto(value message.Status) (deliveryv1.DeliveryStatus, bool) {
	statuses := map[message.Status]deliveryv1.DeliveryStatus{
		message.StatusAccepted:          deliveryv1.DeliveryStatus_DELIVERY_STATUS_ACCEPTED,
		message.StatusScheduled:         deliveryv1.DeliveryStatus_DELIVERY_STATUS_SCHEDULED,
		message.StatusQueued:            deliveryv1.DeliveryStatus_DELIVERY_STATUS_QUEUED,
		message.StatusSending:           deliveryv1.DeliveryStatus_DELIVERY_STATUS_SENDING,
		message.StatusRetryScheduled:    deliveryv1.DeliveryStatus_DELIVERY_STATUS_RETRY_SCHEDULED,
		message.StatusSubmissionUnknown: deliveryv1.DeliveryStatus_DELIVERY_STATUS_SUBMISSION_UNKNOWN,
		message.StatusProviderAccepted:  deliveryv1.DeliveryStatus_DELIVERY_STATUS_PROVIDER_ACCEPTED,
		message.StatusDelivered:         deliveryv1.DeliveryStatus_DELIVERY_STATUS_DELIVERED,
		message.StatusBounced:           deliveryv1.DeliveryStatus_DELIVERY_STATUS_BOUNCED,
		message.StatusComplained:        deliveryv1.DeliveryStatus_DELIVERY_STATUS_COMPLAINED,
		message.StatusCanceled:          deliveryv1.DeliveryStatus_DELIVERY_STATUS_CANCELED,
		message.StatusExpired:           deliveryv1.DeliveryStatus_DELIVERY_STATUS_EXPIRED,
		message.StatusPermanentlyFailed: deliveryv1.DeliveryStatus_DELIVERY_STATUS_PERMANENTLY_FAILED,
		message.StatusDeadLettered:      deliveryv1.DeliveryStatus_DELIVERY_STATUS_DEAD_LETTERED,
		message.StatusUnknownFinal:      deliveryv1.DeliveryStatus_DELIVERY_STATUS_UNKNOWN_FINAL,
	}
	mapped, ok := statuses[value]
	return mapped, ok
}

func failureToProto(value message.Failure) *deliveryv1.FailureInfo {
	categories := map[message.FailureCategory]deliveryv1.FailureCategory{
		message.FailureValidation:        deliveryv1.FailureCategory_FAILURE_CATEGORY_VALIDATION,
		message.FailureAuthentication:    deliveryv1.FailureCategory_FAILURE_CATEGORY_AUTHENTICATION,
		message.FailureRateLimited:       deliveryv1.FailureCategory_FAILURE_CATEGORY_RATE_LIMITED,
		message.FailureRecipientRejected: deliveryv1.FailureCategory_FAILURE_CATEGORY_RECIPIENT_REJECTED,
		message.FailureContentRejected:   deliveryv1.FailureCategory_FAILURE_CATEGORY_CONTENT_REJECTED,
		message.FailureProviderDown:      deliveryv1.FailureCategory_FAILURE_CATEGORY_PROVIDER_UNAVAILABLE,
		message.FailureNetwork:           deliveryv1.FailureCategory_FAILURE_CATEGORY_NETWORK,
		message.FailureTimeoutBeforeSend: deliveryv1.FailureCategory_FAILURE_CATEGORY_TIMEOUT_BEFORE_SEND,
		message.FailureSubmissionUnknown: deliveryv1.FailureCategory_FAILURE_CATEGORY_SUBMISSION_UNKNOWN,
		message.FailureInternal:          deliveryv1.FailureCategory_FAILURE_CATEGORY_INTERNAL,
	}
	return &deliveryv1.FailureInfo{
		Category:  categories[value.Category],
		Code:      value.Code,
		Retryable: value.Retryable,
	}
}

func dispositionFromProto(value deliveryv1.EventAckDisposition) (ports.EventAckDisposition, bool) {
	switch value {
	case deliveryv1.EventAckDisposition_EVENT_ACK_DISPOSITION_ACCEPTED:
		return ports.EventAckAccepted, true
	case deliveryv1.EventAckDisposition_EVENT_ACK_DISPOSITION_DUPLICATE:
		return ports.EventAckDuplicate, true
	case deliveryv1.EventAckDisposition_EVENT_ACK_DISPOSITION_IGNORED_STALE:
		return ports.EventAckIgnoredStale, true
	default:
		return "", false
	}
}

func classifyGRPCError(err error) (string, bool) {
	code := status.Code(err)
	stableCode := "GRPC_" + strings.ToUpper(grpcCodeName(code))
	switch code {
	case codes.Canceled,
		codes.DeadlineExceeded,
		codes.Unknown,
		codes.NotFound,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.Internal,
		codes.Unavailable,
		codes.DataLoss:
		return stableCode, true
	default:
		return stableCode, false
	}
}

func grpcCodeName(code codes.Code) string {
	names := map[codes.Code]string{
		codes.OK:                 "OK",
		codes.Canceled:           "CANCELED",
		codes.Unknown:            "UNKNOWN",
		codes.InvalidArgument:    "INVALID_ARGUMENT",
		codes.DeadlineExceeded:   "DEADLINE_EXCEEDED",
		codes.NotFound:           "NOT_FOUND",
		codes.AlreadyExists:      "ALREADY_EXISTS",
		codes.PermissionDenied:   "PERMISSION_DENIED",
		codes.ResourceExhausted:  "RESOURCE_EXHAUSTED",
		codes.FailedPrecondition: "FAILED_PRECONDITION",
		codes.Aborted:            "ABORTED",
		codes.OutOfRange:         "OUT_OF_RANGE",
		codes.Unimplemented:      "UNIMPLEMENTED",
		codes.Internal:           "INTERNAL",
		codes.Unavailable:        "UNAVAILABLE",
		codes.DataLoss:           "DATA_LOSS",
		codes.Unauthenticated:    "UNAUTHENTICATED",
	}
	if name, ok := names[code]; ok {
		return name
	}
	return "UNKNOWN_CODE"
}

func stringPointer(value string) *string { return &value }
