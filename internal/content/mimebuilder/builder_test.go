package mimebuilder

import (
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"strings"
	"testing"
	"time"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

func TestBuilderCreatesUTF8MultipartAlternativeMessage(t *testing.T) {
	request := ports.MIMEMessageRequest{
		AttemptID: "bb000000-0000-4000-8000-000000000001",
		Date:      time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
		Sender: ports.SenderIdentity{
			Key:         "ainexus.default",
			Address:     "no-reply@example.invalid",
			DisplayName: "AI Nexus",
		},
		RecipientAddress:     "user@example.com",
		RecipientDisplayName: "测试用户",
		Content: ports.RenderedEmail{
			Subject:  "AI Nexus 登录验证码",
			TextBody: "验证码：123456\n五分钟内有效。",
			HTMLBody: "<!doctype html><p>验证码：<strong>123456</strong></p>",
		},
	}
	encoded, err := New().Encode(request)
	if err != nil {
		t.Fatalf("encode MIME: %v", err)
	}
	withoutCRLF := strings.ReplaceAll(string(encoded), "\r\n", "")
	if strings.ContainsAny(withoutCRLF, "\r\n") {
		t.Fatal("MIME message contains bare LF")
	}
	message, err := mail.ReadMessage(strings.NewReader(string(encoded)))
	if err != nil {
		t.Fatalf("parse MIME message: %v", err)
	}
	decodedSubject, err := new(mime.WordDecoder).DecodeHeader(message.Header.Get("Subject"))
	if err != nil || decodedSubject != request.Content.Subject {
		t.Fatalf("subject = %q/%v", decodedSubject, err)
	}
	mediaType, parameters, err := mime.ParseMediaType(message.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/alternative" {
		t.Fatalf("content type = %q %#v/%v", mediaType, parameters, err)
	}
	reader := multipart.NewReader(message.Body, parameters["boundary"])
	wantBodies := []string{request.Content.TextBody, request.Content.HTMLBody}
	for index, want := range wantBodies {
		part, err := reader.NextRawPart()
		if err != nil {
			t.Fatalf("read MIME part %d: %v", index, err)
		}
		decoded, err := io.ReadAll(quotedprintable.NewReader(part))
		if err != nil {
			t.Fatalf("decode MIME part %d: %v", index, err)
		}
		if normalizeNewlines(string(decoded)) != normalizeNewlines(want) {
			t.Fatalf("part %d body = %q, want %q", index, decoded, want)
		}
	}
	if _, err := reader.NextPart(); err != io.EOF {
		t.Fatalf("unexpected extra MIME part: %v", err)
	}
}

func TestBuilderRejectsHeaderInjection(t *testing.T) {
	request := ports.MIMEMessageRequest{
		AttemptID:        "bb000000-0000-4000-8000-000000000001",
		Date:             time.Now(),
		Sender:           ports.SenderIdentity{Key: "sender", Address: "sender@example.com"},
		RecipientAddress: "user@example.com",
		Content: ports.RenderedEmail{
			Subject:  "safe\r\nBcc: victim@example.com",
			TextBody: "text",
			HTMLBody: "<p>text</p>",
		},
	}
	if _, err := New().Encode(request); err == nil {
		t.Fatal("header injection was accepted")
	}
	request.Content.Subject = "safe"
	request.RecipientDisplayName = "name\r\nBcc: victim@example.com"
	if _, err := New().Encode(request); err == nil {
		t.Fatal("recipient header injection was accepted")
	}
}

func normalizeNewlines(value string) string {
	return strings.ReplaceAll(value, "\r\n", "\n")
}
