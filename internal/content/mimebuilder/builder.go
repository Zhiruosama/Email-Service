// Package mimebuilder encodes a rendered email into an RFC-style UTF-8
// multipart/alternative message. SMTP envelope handling stays in Providers.
package mimebuilder

import (
	"bytes"
	"fmt"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"net/textproto"
	"strings"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
	"github.com/google/uuid"
)

type Builder struct{}

var _ ports.MIMEEncoder = (*Builder)(nil)

func New() *Builder { return &Builder{} }

func (b *Builder) Encode(request ports.MIMEMessageRequest) ([]byte, error) {
	if _, err := uuid.Parse(request.AttemptID); err != nil || request.Date.IsZero() {
		return nil, fmt.Errorf("%w: attempt identity and date are required", ports.ErrInvalidDeliveryMaterial)
	}
	if err := request.Sender.Validate(); err != nil {
		return nil, err
	}
	if err := request.Content.Validate(); err != nil {
		return nil, err
	}
	if !validRecipient(request.RecipientAddress, request.RecipientDisplayName) {
		return nil, fmt.Errorf("%w: recipient is invalid", ports.ErrInvalidDeliveryMaterial)
	}

	boundary := "mail-" + strings.ReplaceAll(request.AttemptID, "-", "")
	from := (&mail.Address{Name: request.Sender.DisplayName, Address: request.Sender.Address}).String()
	to := (&mail.Address{Name: request.RecipientDisplayName, Address: request.RecipientAddress}).String()
	headers := [][2]string{
		{"Date", request.Date.UTC().Format("Mon, 02 Jan 2006 15:04:05 -0700")},
		{"Message-ID", "<" + request.AttemptID + "@mail-service.local>"},
		{"From", from},
		{"To", to},
		{"Subject", mime.QEncoding.Encode("UTF-8", request.Content.Subject)},
		{"MIME-Version", "1.0"},
		{"Content-Type", `multipart/alternative; boundary="` + boundary + `"`},
	}
	var message bytes.Buffer
	for _, header := range headers {
		if strings.ContainsAny(header[0]+header[1], "\r\n") {
			return nil, fmt.Errorf("%w: MIME header is invalid", ports.ErrInvalidDeliveryMaterial)
		}
		message.WriteString(header[0])
		message.WriteString(": ")
		message.WriteString(header[1])
		message.WriteString("\r\n")
	}
	message.WriteString("\r\n")
	multipartWriter := multipart.NewWriter(&message)
	if err := multipartWriter.SetBoundary(boundary); err != nil {
		return nil, fmt.Errorf("%w: MIME boundary is invalid", ports.ErrInvalidDeliveryMaterial)
	}
	if err := writeAlternativePart(multipartWriter, "text/plain; charset=UTF-8", request.Content.TextBody); err != nil {
		return nil, err
	}
	if err := writeAlternativePart(multipartWriter, "text/html; charset=UTF-8", request.Content.HTMLBody); err != nil {
		return nil, err
	}
	if err := multipartWriter.Close(); err != nil {
		return nil, fmt.Errorf("%w: close MIME body", ports.ErrInvalidDeliveryMaterial)
	}
	encoded := message.Bytes()
	material := ports.DeliveryMaterial{
		EnvelopeFrom: request.Sender.Address,
		EnvelopeTo:   request.RecipientAddress,
		MIMEMessage:  encoded,
	}
	if err := material.Validate(); err != nil {
		return nil, err
	}
	return encoded, nil
}

func writeAlternativePart(writer *multipart.Writer, contentType, content string) error {
	header := make(textproto.MIMEHeader)
	header.Set("Content-Type", contentType)
	header.Set("Content-Transfer-Encoding", "quoted-printable")
	part, err := writer.CreatePart(header)
	if err != nil {
		return fmt.Errorf("%w: create MIME part", ports.ErrInvalidDeliveryMaterial)
	}
	encoder := quotedprintable.NewWriter(part)
	if _, err := encoder.Write([]byte(toCRLF(content))); err != nil {
		return fmt.Errorf("%w: encode MIME part", ports.ErrInvalidDeliveryMaterial)
	}
	if err := encoder.Close(); err != nil {
		return fmt.Errorf("%w: close MIME part", ports.ErrInvalidDeliveryMaterial)
	}
	return nil
}

func validRecipient(address, displayName string) bool {
	if len(displayName) > 256 || strings.ContainsAny(displayName, "\r\n") {
		return false
	}
	parsed, err := mail.ParseAddress(address)
	return err == nil && parsed.Name == "" && parsed.Address == address
}

func toCRLF(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	return strings.ReplaceAll(value, "\n", "\r\n")
}
