// Package rabbitmq defines the shared AMQP contract between publisher and
// consumer adapters. It contains no network client or lifecycle behavior.
package rabbitmq

const (
	ExchangeEvents = "mail.events.v1"
	ExchangeDead   = "mail.dead.v1"

	QueueDispatch      = "mail.dispatch.v1.q"
	QueueLifecycle     = "mail.lifecycle.v1.q"
	QueueDispatchDead  = "mail.dispatch.dead.v1.q"
	QueueLifecycleDead = "mail.lifecycle.dead.v1.q"

	RoutingMessageAccepted   = "mail.message.accepted.v1"
	RoutingStatusChanged     = "mail.message.status.changed.v1"
	RoutingDispatchRequested = "mail.message.dispatch.requested.v1"
	RoutingDispatchDead      = "mail.dispatch.dead.v1"
	RoutingLifecycleDead     = "mail.lifecycle.dead.v1"

	EventMessageAccepted   = "MESSAGE_ACCEPTED"
	EventStatusChanged     = "MESSAGE_STATUS_CHANGED"
	EventDispatchRequested = "MESSAGE_DISPATCH_REQUESTED"

	ContentTypeJSON = "application/json"

	HeaderAggregateType      = "x-mail-aggregate-type"
	HeaderAggregateID        = "x-mail-aggregate-id"
	HeaderAggregateSequence  = "x-mail-aggregate-sequence"
	HeaderDispatchGeneration = "x-mail-dispatch-generation"
	HeaderPublishAttempt     = "x-mail-publish-attempt"
)
