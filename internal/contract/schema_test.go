package contract_test

import (
	"strings"
	"testing"

	deliveryv1 "github.com/Zhiruosama/Email-Service/gen/go/mailservice/delivery/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestDeliveryServiceMethodNamesAreFrozen(t *testing.T) {
	t.Parallel()

	service := deliveryv1.File_mailservice_delivery_v1_delivery_proto.
		Services().
		ByName("DeliveryService")
	if service == nil {
		t.Fatal("DeliveryService descriptor is missing")
	}

	want := []protoreflect.Name{
		"SubmitEmail",
		"BatchSubmitEmail",
		"GetEmail",
		"CancelEmail",
		"ListEmailEvents",
	}
	if service.Methods().Len() != len(want) {
		t.Fatalf("DeliveryService method count = %d, want %d", service.Methods().Len(), len(want))
	}
	for index, name := range want {
		if got := service.Methods().Get(index).Name(); got != name {
			t.Errorf("DeliveryService method %d = %q, want %q", index, got, name)
		}
	}
}

func TestDeliveryEventReceiverMethodNameIsFrozen(t *testing.T) {
	t.Parallel()

	service := deliveryv1.File_mailservice_delivery_v1_event_proto.
		Services().
		ByName("DeliveryEventReceiverService")
	if service == nil {
		t.Fatal("DeliveryEventReceiverService descriptor is missing")
	}
	if service.Methods().Len() != 1 {
		t.Fatalf("receiver method count = %d, want 1", service.Methods().Len())
	}
	if got := service.Methods().Get(0).Name(); got != "ReportDeliveryEvent" {
		t.Fatalf("receiver method = %q, want ReportDeliveryEvent", got)
	}
}

func TestSubmitEmailFieldNumbersAreFrozen(t *testing.T) {
	t.Parallel()

	message := deliveryv1.File_mailservice_delivery_v1_delivery_proto.
		Messages().
		ByName("SubmitEmailRequest")
	if message == nil {
		t.Fatal("SubmitEmailRequest descriptor is missing")
	}

	want := map[protoreflect.Name]protoreflect.FieldNumber{
		"idempotency_key":       1,
		"recipient":             2,
		"sender_identity_key":   3,
		"content":               4,
		"category":              5,
		"priority":              6,
		"scheduled_at":          7,
		"dispatch_deadline":     8,
		"duplicate_risk_policy": 9,
		"metadata":              10,
	}
	for name, number := range want {
		field := message.Fields().ByName(name)
		if field == nil {
			t.Errorf("field %q is missing", name)
			continue
		}
		if field.Number() != number {
			t.Errorf("field %q number = %d, want %d", name, field.Number(), number)
		}
	}
}

func TestDeliveryStatusNumbersAreFrozen(t *testing.T) {
	t.Parallel()

	enum := deliveryv1.File_mailservice_delivery_v1_common_proto.
		Enums().
		ByName("DeliveryStatus")
	if enum == nil {
		t.Fatal("DeliveryStatus descriptor is missing")
	}

	want := map[protoreflect.Name]protoreflect.EnumNumber{
		"DELIVERY_STATUS_UNSPECIFIED":        0,
		"DELIVERY_STATUS_ACCEPTED":           1,
		"DELIVERY_STATUS_SCHEDULED":          2,
		"DELIVERY_STATUS_QUEUED":             3,
		"DELIVERY_STATUS_SENDING":            4,
		"DELIVERY_STATUS_RETRY_SCHEDULED":    5,
		"DELIVERY_STATUS_SUBMISSION_UNKNOWN": 6,
		"DELIVERY_STATUS_PROVIDER_ACCEPTED":  7,
		"DELIVERY_STATUS_DELIVERED":          8,
		"DELIVERY_STATUS_BOUNCED":            9,
		"DELIVERY_STATUS_COMPLAINED":         10,
		"DELIVERY_STATUS_CANCELED":           11,
		"DELIVERY_STATUS_EXPIRED":            12,
		"DELIVERY_STATUS_PERMANENTLY_FAILED": 13,
		"DELIVERY_STATUS_DEAD_LETTERED":      14,
		"DELIVERY_STATUS_UNKNOWN_FINAL":      15,
	}
	for name, number := range want {
		value := enum.Values().ByName(name)
		if value == nil {
			t.Errorf("enum value %q is missing", name)
			continue
		}
		if value.Number() != number {
			t.Errorf("enum value %q number = %d, want %d", name, value.Number(), number)
		}
	}
}

func TestAllTopLevelEnumsHaveUnspecifiedZeroValue(t *testing.T) {
	t.Parallel()

	files := []protoreflect.FileDescriptor{
		deliveryv1.File_mailservice_delivery_v1_common_proto,
		deliveryv1.File_mailservice_delivery_v1_delivery_proto,
		deliveryv1.File_mailservice_delivery_v1_event_proto,
	}
	for _, file := range files {
		enums := file.Enums()
		for index := 0; index < enums.Len(); index++ {
			enum := enums.Get(index)
			zero := enum.Values().ByNumber(0)
			if zero == nil {
				t.Errorf("%s has no zero value", enum.FullName())
				continue
			}
			if !strings.HasSuffix(string(zero.Name()), "_UNSPECIFIED") {
				t.Errorf("%s zero value = %s, want *_UNSPECIFIED", enum.FullName(), zero.Name())
			}
		}
	}
}

func TestCoreSubmissionContractDoesNotExposeTenantOrTransportControls(t *testing.T) {
	t.Parallel()

	message := deliveryv1.File_mailservice_delivery_v1_delivery_proto.
		Messages().
		ByName("SubmitEmailRequest")
	for _, forbidden := range []protoreflect.Name{
		"tenant_id",
		"callback_url",
		"subject",
		"html",
		"smtp_host",
	} {
		if field := message.Fields().ByName(forbidden); field != nil {
			t.Errorf("SubmitEmailRequest must not expose %q", forbidden)
		}
	}
}
