package smtp

import (
	"context"
	"errors"
	"fmt"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/Zhiruosama/Email-Service/internal/domain/message"
)

type Provider struct {
	config    Config
	transport Transport
}

var _ ports.EmailProvider = (*Provider)(nil)

func New(config Config) (*Provider, error) {
	transport, err := NewClientTransport(config)
	if err != nil {
		return nil, err
	}
	return newWithTransport(config, transport)
}

func newWithTransport(config Config, transport Transport) (*Provider, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if transport == nil {
		panic("smtp: nil transport")
	}
	return &Provider{config: config, transport: transport}, nil
}

func (p *Provider) Key() string { return ProviderKey }

func (p *Provider) Submit(ctx context.Context, request ports.ProviderRequest) ports.ProviderResult {
	if err := request.Validate(); err != nil || request.Material.EnvelopeFrom != p.config.FromAddress {
		return failed(message.FailureValidation, "SMTP_REQUEST_INVALID", false)
	}
	err := p.transport.Deliver(ctx, request.Material)
	if err == nil {
		return ports.ProviderResult{
			Outcome:           ports.ProviderOutcomeAccepted,
			ProviderMessageID: "smtp/" + request.AttemptID,
		}
	}
	return classifyFailure(err)
}

func classifyFailure(err error) ports.ProviderResult {
	var exchange *ExchangeError
	if !errors.As(err, &exchange) {
		return failed(message.FailureProviderDown, "SMTP_TRANSPORT_INTERNAL", true)
	}
	if exchange.Phase == PhaseDataCommit && exchange.StatusCode == 0 {
		failure := message.Failure{
			Category:  message.FailureSubmissionUnknown,
			Code:      "SMTP_DATA_COMMIT_RESULT_UNKNOWN",
			Retryable: false,
		}
		return ports.ProviderResult{
			Outcome: ports.ProviderOutcomeSubmissionUnknown,
			Failure: &failure,
		}
	}
	if exchange.StatusCode >= 400 && exchange.StatusCode <= 499 {
		category := message.FailureProviderDown
		if exchange.StatusCode == 450 || exchange.StatusCode == 452 {
			category = message.FailureRateLimited
		}
		return failed(category, statusCode(exchange), true)
	}
	if exchange.StatusCode >= 500 && exchange.StatusCode <= 599 {
		category := message.FailureProviderDown
		switch exchange.Phase {
		case PhaseAuth:
			category = message.FailureAuthentication
		case PhaseMailFrom:
			category = message.FailureValidation
		case PhaseRecipient:
			category = message.FailureRecipientRejected
		case PhaseData, PhaseDataCommit:
			category = message.FailureContentRejected
		}
		return failed(category, statusCode(exchange), false)
	}
	if exchange.Phase == PhaseAuth && exchange.Protocol {
		return failed(
			message.FailureAuthentication,
			"SMTP_AUTH_MECHANISM_UNAVAILABLE",
			false,
		)
	}
	if exchange.TimedOut {
		return failed(message.FailureTimeoutBeforeSend, "SMTP_TIMEOUT_BEFORE_COMMIT", true)
	}
	if exchange.Canceled {
		return failed(message.FailureNetwork, "SMTP_CANCELED_BEFORE_COMMIT", true)
	}
	return failed(message.FailureNetwork, "SMTP_NETWORK_BEFORE_COMMIT", true)
}

func statusCode(exchange *ExchangeError) string {
	return fmt.Sprintf("SMTP_%s_%d", exchange.Phase, exchange.StatusCode)
}

func failed(category message.FailureCategory, code string, retryable bool) ports.ProviderResult {
	failure := message.Failure{Category: category, Code: code, Retryable: retryable}
	return ports.ProviderResult{Outcome: ports.ProviderOutcomeFailed, Failure: &failure}
}
