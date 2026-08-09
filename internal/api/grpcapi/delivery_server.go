package grpcapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	deliveryv1 "github.com/Zhiruosama/Email-Service/gen/go/mailservice/delivery/v1"
	deliveryapp "github.com/Zhiruosama/Email-Service/internal/application/delivery"
	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type EmailSubmitter interface {
	Submit(context.Context, deliveryapp.SubmitEmailCommand) (deliveryapp.SubmitEmailResult, error)
}

type EmailQuerier interface {
	Get(context.Context, deliveryapp.GetEmailQuery) (ports.MessageRecord, error)
}

type DeliveryServer struct {
	deliveryv1.UnimplementedDeliveryServiceServer
	submitter EmailSubmitter
	querier   EmailQuerier
}

var _ deliveryv1.DeliveryServiceServer = (*DeliveryServer)(nil)

func NewDeliveryServer(submitter EmailSubmitter, querier EmailQuerier) *DeliveryServer {
	if submitter == nil || querier == nil {
		panic("grpcapi: delivery service dependencies must not be nil")
	}
	return &DeliveryServer{submitter: submitter, querier: querier}
}

func (s *DeliveryServer) SubmitEmail(
	ctx context.Context,
	request *deliveryv1.SubmitEmailRequest,
) (*deliveryv1.SubmitEmailResponse, error) {
	tenantID, err := TenantIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	command, err := submitCommandFromProto(tenantID, request)
	if err != nil {
		return nil, publicError(err)
	}
	result, err := s.submitter.Submit(ctx, command)
	if err != nil {
		return nil, publicError(err)
	}
	view, err := messageToProto(result.Record)
	if err != nil {
		return nil, publicError(err)
	}
	disposition := deliveryv1.SubmitDisposition_SUBMIT_DISPOSITION_ACCEPTED
	if result.Disposition == deliveryapp.SubmitEmailDuplicate {
		disposition = deliveryv1.SubmitDisposition_SUBMIT_DISPOSITION_DUPLICATE
	} else if result.Disposition != deliveryapp.SubmitEmailAccepted {
		return nil, publicError(fmt.Errorf("%w: unknown submit disposition", deliveryapp.ErrSubmissionInvariant))
	}
	return &deliveryv1.SubmitEmailResponse{Disposition: disposition, Message: view}, nil
}

func (s *DeliveryServer) GetEmail(
	ctx context.Context,
	request *deliveryv1.GetEmailRequest,
) (*deliveryv1.GetEmailResponse, error) {
	tenantID, err := TenantIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	query, err := getQueryFromProto(tenantID, request)
	if err != nil {
		return nil, publicError(err)
	}
	record, err := s.querier.Get(ctx, query)
	if err != nil {
		return nil, publicError(err)
	}
	view, err := messageToProto(record)
	if err != nil {
		return nil, publicError(err)
	}
	return &deliveryv1.GetEmailResponse{Message: view}, nil
}

func submitCommandFromProto(
	tenantID string,
	request *deliveryv1.SubmitEmailRequest,
) (deliveryapp.SubmitEmailCommand, error) {
	if request == nil || request.Recipient == nil || request.Content == nil ||
		request.Content.Template == nil || request.Content.Variables == nil {
		return deliveryapp.SubmitEmailCommand{}, fmt.Errorf("%w: recipient, content, template and variables are required", deliveryapp.ErrInvalidSubmission)
	}
	deadline, err := requiredTimestamp(request.DispatchDeadline, "dispatch deadline")
	if err != nil {
		return deliveryapp.SubmitEmailCommand{}, err
	}
	var scheduledAt *time.Time
	if request.ScheduledAt != nil {
		value, timestampErr := requiredTimestamp(request.ScheduledAt, "scheduled time")
		if timestampErr != nil {
			return deliveryapp.SubmitEmailCommand{}, timestampErr
		}
		scheduledAt = &value
	}
	variables, err := json.Marshal(request.Content.Variables.AsMap())
	if err != nil {
		return deliveryapp.SubmitEmailCommand{}, fmt.Errorf("%w: template variables cannot be encoded", deliveryapp.ErrInvalidSubmission)
	}
	category, err := categoryFromProto(request.Category)
	if err != nil {
		return deliveryapp.SubmitEmailCommand{}, err
	}
	risk, err := duplicateRiskFromProto(request.DuplicateRiskPolicy)
	if err != nil {
		return deliveryapp.SubmitEmailCommand{}, err
	}
	if request.Priority > 9 {
		return deliveryapp.SubmitEmailCommand{}, fmt.Errorf("%w: priority must be in range 0..9", deliveryapp.ErrInvalidSubmission)
	}
	return deliveryapp.SubmitEmailCommand{
		TenantID:             tenantID,
		IdempotencyKey:       request.IdempotencyKey,
		RecipientEmail:       request.Recipient.Email,
		RecipientDisplayName: request.Recipient.GetDisplayName(),
		SenderIdentityKey:    request.SenderIdentityKey,
		TemplateKey:          request.Content.Template.Key,
		TemplateVersion:      cloneUint32(request.Content.Template.Version),
		Locale:               request.Content.Locale,
		Variables:            variables,
		Category:             category,
		Priority:             uint8(request.Priority),
		ScheduledAt:          scheduledAt,
		DispatchDeadline:     deadline,
		DuplicateRiskPolicy:  risk,
		Metadata:             cloneMetadata(request.Metadata),
	}, nil
}

func getQueryFromProto(
	tenantID string,
	request *deliveryv1.GetEmailRequest,
) (deliveryapp.GetEmailQuery, error) {
	if request == nil {
		return deliveryapp.GetEmailQuery{}, fmt.Errorf("%w: request is required", deliveryapp.ErrInvalidEmailQuery)
	}
	query := deliveryapp.GetEmailQuery{TenantID: tenantID}
	switch selector := request.Selector.(type) {
	case *deliveryv1.GetEmailRequest_MessageId:
		query.MessageID = selector.MessageId
	case *deliveryv1.GetEmailRequest_IdempotencyKey:
		query.IdempotencyKey = selector.IdempotencyKey
	default:
		return deliveryapp.GetEmailQuery{}, fmt.Errorf("%w: selector is required", deliveryapp.ErrInvalidEmailQuery)
	}
	return query, nil
}

func requiredTimestamp(value *timestamppb.Timestamp, field string) (time.Time, error) {
	if value == nil {
		return time.Time{}, fmt.Errorf("%w: %s is required", deliveryapp.ErrInvalidSubmission, field)
	}
	if err := value.CheckValid(); err != nil {
		return time.Time{}, fmt.Errorf("%w: %s is invalid", deliveryapp.ErrInvalidSubmission, field)
	}
	return value.AsTime().UTC(), nil
}

func categoryFromProto(value deliveryv1.EmailCategory) (ports.EmailCategory, error) {
	switch value {
	case deliveryv1.EmailCategory_EMAIL_CATEGORY_CRITICAL:
		return ports.EmailCategoryCritical, nil
	case deliveryv1.EmailCategory_EMAIL_CATEGORY_TRANSACTIONAL:
		return ports.EmailCategoryTransactional, nil
	case deliveryv1.EmailCategory_EMAIL_CATEGORY_NOTIFICATION:
		return ports.EmailCategoryNotification, nil
	case deliveryv1.EmailCategory_EMAIL_CATEGORY_BULK:
		return ports.EmailCategoryBulk, nil
	default:
		return "", fmt.Errorf("%w: email category is required", deliveryapp.ErrInvalidSubmission)
	}
}

func duplicateRiskFromProto(value deliveryv1.DuplicateRiskPolicy) (ports.DuplicateRiskPolicy, error) {
	switch value {
	case deliveryv1.DuplicateRiskPolicy_DUPLICATE_RISK_POLICY_AVOID_DUPLICATE:
		return ports.DuplicateRiskAvoidDuplicate, nil
	case deliveryv1.DuplicateRiskPolicy_DUPLICATE_RISK_POLICY_PREFER_DELIVERY:
		return ports.DuplicateRiskPreferDelivery, nil
	default:
		return "", fmt.Errorf("%w: duplicate risk policy is required", deliveryapp.ErrInvalidSubmission)
	}
}

func messageToProto(record ports.MessageRecord) (*deliveryv1.EmailMessage, error) {
	if err := record.Validate(); err != nil || record.Submission == nil {
		return nil, fmt.Errorf("%w: persisted message has no safe submission view", deliveryapp.ErrSubmissionInvariant)
	}
	snapshot := record.Message.Snapshot()
	statusValue, err := statusToProto(snapshot.Status)
	if err != nil {
		return nil, err
	}
	view := &deliveryv1.EmailMessage{
		MessageId:      record.Message.ID(),
		IdempotencyKey: record.IdempotencyKey,
		Recipient: &deliveryv1.RecipientSummary{
			MaskedEmail: record.Submission.RecipientMasked,
		},
		SenderIdentityKey: record.Submission.SenderIdentityKey,
		Template: &deliveryv1.TemplateReference{
			Key:     record.Submission.TemplateKey,
			Version: uint32Pointer(record.Submission.TemplateVersion),
		},
		Locale:           record.Submission.Locale,
		Category:         categoryToProto(record.Category),
		Priority:         uint32(record.Priority),
		Status:           statusValue,
		ScheduledAt:      optionalTimestamp(snapshot.ScheduledAt),
		DispatchDeadline: timestamppb.New(snapshot.DispatchDeadline),
		AcceptedAt:       timestamppb.New(snapshot.AcceptedAt),
		UpdatedAt:        timestamppb.New(snapshot.UpdatedAt),
		LatestSequence:   snapshot.LatestSequence,
	}
	if snapshot.LastFailure != nil {
		view.LatestFailure = failureToProto(*snapshot.LastFailure)
	}
	return view, nil
}

func statusToProto(value message.Status) (deliveryv1.DeliveryStatus, error) {
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
	if !ok {
		return 0, fmt.Errorf("%w: unknown message status", deliveryapp.ErrSubmissionInvariant)
	}
	return mapped, nil
}

func categoryToProto(value ports.EmailCategory) deliveryv1.EmailCategory {
	switch value {
	case ports.EmailCategoryCritical:
		return deliveryv1.EmailCategory_EMAIL_CATEGORY_CRITICAL
	case ports.EmailCategoryTransactional:
		return deliveryv1.EmailCategory_EMAIL_CATEGORY_TRANSACTIONAL
	case ports.EmailCategoryNotification:
		return deliveryv1.EmailCategory_EMAIL_CATEGORY_NOTIFICATION
	case ports.EmailCategoryBulk:
		return deliveryv1.EmailCategory_EMAIL_CATEGORY_BULK
	default:
		return deliveryv1.EmailCategory_EMAIL_CATEGORY_UNSPECIFIED
	}
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

func publicError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	}
	switch {
	case errors.Is(err, deliveryapp.ErrInvalidSubmission),
		errors.Is(err, deliveryapp.ErrInvalidEmailQuery),
		errors.Is(err, ports.ErrInvalidMessageRecord),
		errors.Is(err, ports.ErrTemplateVariables):
		return status.Error(codes.InvalidArgument, "request validation failed")
	case errors.Is(err, ports.ErrTemplateNotAllowed), errors.Is(err, ports.ErrSenderIdentityNotAllowed):
		return status.Error(codes.PermissionDenied, "template is not allowed")
	case errors.Is(err, ports.ErrTemplateNotFound), errors.Is(err, ports.ErrMessageNotFound):
		return status.Error(codes.NotFound, "requested resource was not found")
	case errors.Is(err, ports.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, "idempotency key is already bound to another payload")
	case errors.Is(err, ports.ErrTransaction), errors.Is(err, ports.ErrMessageRepository):
		return status.Error(codes.Unavailable, "service storage is temporarily unavailable")
	default:
		return status.Error(codes.Internal, "internal service error")
	}
}

func optionalTimestamp(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamppb.New(value.UTC())
}

func uint32Pointer(value uint32) *uint32 { return &value }

func cloneUint32(value *uint32) *uint32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneMetadata(value map[string]string) map[string]string {
	if value == nil {
		return nil
	}
	copy := make(map[string]string, len(value))
	for key, item := range value {
		copy[key] = item
	}
	return copy
}
