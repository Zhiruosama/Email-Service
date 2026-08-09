package smtp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	netsmtp "net/smtp"
	"net/textproto"
	"strings"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

type ExchangePhase string

const (
	PhaseConnect    ExchangePhase = "CONNECT"
	PhaseTLS        ExchangePhase = "TLS"
	PhaseGreeting   ExchangePhase = "GREETING"
	PhaseAuth       ExchangePhase = "AUTH"
	PhaseMailFrom   ExchangePhase = "MAIL_FROM"
	PhaseRecipient  ExchangePhase = "RECIPIENT"
	PhaseData       ExchangePhase = "DATA"
	PhaseDataWrite  ExchangePhase = "DATA_WRITE"
	PhaseDataCommit ExchangePhase = "DATA_COMMIT"
)

var ErrAuthMechanismUnavailable = errors.New("SMTP authentication mechanism unavailable")

// ExchangeError deliberately excludes the SMTP response text because remote
// servers can echo addresses or content. Phase and numeric status are enough
// for stable retry classification.
type ExchangeError struct {
	Phase      ExchangePhase
	StatusCode int
	TimedOut   bool
	Canceled   bool
	Network    bool
	Protocol   bool
}

func (e *ExchangeError) Error() string {
	if e == nil {
		return "SMTP exchange failed"
	}
	if e.StatusCode != 0 {
		return fmt.Sprintf("SMTP exchange failed at %s with status %d", e.Phase, e.StatusCode)
	}
	return fmt.Sprintf("SMTP exchange failed at %s", e.Phase)
}

type Transport interface {
	Deliver(context.Context, ports.DeliveryMaterial) error
}

type ClientTransport struct {
	config Config
	roots  *x509.CertPool
}

var _ Transport = (*ClientTransport)(nil)

func NewClientTransport(config Config) (*ClientTransport, error) {
	return newClientTransport(config, nil)
}

func newClientTransport(config Config, roots *x509.CertPool) (*ClientTransport, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &ClientTransport{config: config, roots: roots}, nil
}

func (t *ClientTransport) Deliver(
	ctx context.Context,
	material ports.DeliveryMaterial,
) error {
	if err := material.Validate(); err != nil || material.EnvelopeFrom != t.config.FromAddress {
		return &ExchangeError{Phase: PhaseMailFrom}
	}
	if err := ctx.Err(); err != nil {
		return exchangeError(ctx, PhaseConnect, err)
	}

	deadline := time.Now().Add(t.config.SessionTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	dialer := net.Dialer{Deadline: deadline}
	rawConnection, err := dialer.DialContext(ctx, "tcp", t.config.Address())
	if err != nil {
		return exchangeError(ctx, PhaseConnect, err)
	}
	defer rawConnection.Close()
	if err := rawConnection.SetDeadline(deadline); err != nil {
		return exchangeError(ctx, PhaseConnect, err)
	}

	tlsConnection := tls.Client(rawConnection, &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: t.config.Host,
		RootCAs:    t.roots,
	})
	if err := tlsConnection.HandshakeContext(ctx); err != nil {
		return exchangeError(ctx, PhaseTLS, err)
	}
	stopCancellation := context.AfterFunc(ctx, func() {
		_ = tlsConnection.SetDeadline(time.Now())
	})
	defer stopCancellation()

	client, err := netsmtp.NewClient(tlsConnection, t.config.Host)
	if err != nil {
		return exchangeError(ctx, PhaseGreeting, err)
	}
	defer client.Close()

	if err := client.Auth(t.auth()); err != nil {
		return exchangeError(ctx, PhaseAuth, err)
	}
	if err := client.Mail(material.EnvelopeFrom); err != nil {
		return exchangeError(ctx, PhaseMailFrom, err)
	}
	if err := client.Rcpt(material.EnvelopeTo); err != nil {
		return exchangeError(ctx, PhaseRecipient, err)
	}
	dataWriter, err := client.Data()
	if err != nil {
		return exchangeError(ctx, PhaseData, err)
	}
	written, err := dataWriter.Write(material.MIMEMessage)
	if err == nil && written != len(material.MIMEMessage) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return exchangeError(ctx, PhaseDataWrite, err)
	}
	if err := dataWriter.Close(); err != nil {
		return exchangeError(ctx, PhaseDataCommit, err)
	}
	// The final DATA response is authoritative. A subsequent QUIT failure does
	// not undo acceptance and therefore must not change the result.
	_ = client.Quit()
	return nil
}

func (t *ClientTransport) auth() netsmtp.Auth {
	if t.config.AuthMethod == AuthPlain {
		return netsmtp.PlainAuth("", t.config.Username, t.config.AuthCode, t.config.Host)
	}
	return &loginAuth{username: t.config.Username, password: t.config.AuthCode}
}

type loginAuth struct {
	username string
	password string
	step     uint8
}

func (a *loginAuth) Start(server *netsmtp.ServerInfo) (string, []byte, error) {
	if !server.TLS || !containsFold(server.Auth, "LOGIN") {
		return "", nil, ErrAuthMechanismUnavailable
	}
	a.step = 0
	return "LOGIN", nil, nil
}

func (a *loginAuth) Next(_ []byte, more bool) ([]byte, error) {
	if !more {
		return nil, nil
	}
	switch a.step {
	case 0:
		a.step++
		return []byte(a.username), nil
	case 1:
		a.step++
		return []byte(a.password), nil
	default:
		return nil, ErrAuthMechanismUnavailable
	}
}

func containsFold(values []string, want string) bool {
	for _, value := range values {
		if strings.EqualFold(value, want) {
			return true
		}
	}
	return false
}

func exchangeError(ctx context.Context, phase ExchangePhase, err error) *ExchangeError {
	result := &ExchangeError{Phase: phase}
	var protocolError *textproto.Error
	if errors.As(err, &protocolError) {
		result.StatusCode = protocolError.Code
	}
	contextErr := ctx.Err()
	result.TimedOut = errors.Is(contextErr, context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded)
	result.Canceled = errors.Is(contextErr, context.Canceled) || errors.Is(err, context.Canceled)
	var networkError net.Error
	result.Network = errors.As(err, &networkError)
	if result.Network && networkError.Timeout() {
		result.TimedOut = true
	}
	result.Protocol = errors.Is(err, ErrAuthMechanismUnavailable)
	return result
}
