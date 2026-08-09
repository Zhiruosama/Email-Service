// Package catalog provides the first immutable in-process template catalog.
// A database-backed control-plane adapter can replace it through the same port.
package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"regexp"
	"strings"
	texttemplate "text/template"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

const VerificationCodeTemplateKey = "verification_code.v1"
const AINexusSenderIdentityKey = "ainexus.default"

var sixDigits = regexp.MustCompile(`^[0-9]{6}$`)

type Catalog struct {
	tenantID string
}

var _ ports.TemplateResolver = (*Catalog)(nil)
var _ ports.SubmissionCatalog = (*Catalog)(nil)
var _ ports.DeliveryTemplateRenderer = (*Catalog)(nil)

func NewVerificationCatalog(tenantID string) *Catalog {
	if tenantID == "" {
		panic("catalog: tenant id is required")
	}
	return &Catalog{tenantID: tenantID}
}

func (c *Catalog) Resolve(
	_ context.Context,
	request ports.ResolveTemplateRequest,
) (ports.ResolvedTemplate, error) {
	if request.TenantID != c.tenantID {
		return ports.ResolvedTemplate{}, ports.ErrTemplateNotAllowed
	}
	if request.TemplateKey != VerificationCodeTemplateKey || request.Locale != "zh-CN" {
		return ports.ResolvedTemplate{}, ports.ErrTemplateNotFound
	}
	const activeVersion uint32 = 1
	if request.RequestedVersion != nil && *request.RequestedVersion != activeVersion {
		return ports.ResolvedTemplate{}, ports.ErrTemplateNotFound
	}
	variables, err := validateVerificationVariables(request.Variables)
	if err != nil {
		return ports.ResolvedTemplate{}, err
	}
	return ports.ResolvedTemplate{
		Key:       VerificationCodeTemplateKey,
		Version:   activeVersion,
		Locale:    "zh-CN",
		Variables: variables,
	}, nil
}

func (c *Catalog) AuthorizeSender(_ context.Context, tenantID, senderKey string) error {
	if tenantID != c.tenantID || senderKey != AINexusSenderIdentityKey {
		return ports.ErrSenderIdentityNotAllowed
	}
	return nil
}

type verificationVariables struct {
	Code            string `json:"code"`
	Purpose         string `json:"purpose"`
	ValidForSeconds uint32 `json:"valid_for_seconds"`
}

type verificationView struct {
	Code            string
	Purpose         string
	ValidForSeconds uint32
}

var verificationTextTemplate = texttemplate.Must(texttemplate.New("verification-text").Parse(
	`您的 AI Nexus {{.Purpose}}验证码是：{{.Code}}

验证码将在 {{.ValidForSeconds}} 秒内有效。请勿向任何人透露此验证码。
如果这不是您的操作，请忽略本邮件。`,
))

var verificationHTMLTemplate = template.Must(template.New("verification-html").Parse(
	`<!doctype html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><title>AI Nexus 验证码</title></head>
<body>
  <main>
    <h1>AI Nexus {{.Purpose}}验证</h1>
    <p>您的验证码是：</p>
    <p style="font-size: 28px; font-weight: bold; letter-spacing: 6px;">{{.Code}}</p>
    <p>验证码将在 {{.ValidForSeconds}} 秒内有效。请勿向任何人透露此验证码。</p>
    <p>如果这不是您的操作，请忽略本邮件。</p>
  </main>
</body>
</html>`,
))

func (c *Catalog) RenderDelivery(
	ctx context.Context,
	request ports.RenderDeliveryRequest,
) (ports.RenderedEmail, error) {
	version := request.TemplateVersion
	resolved, err := c.Resolve(ctx, ports.ResolveTemplateRequest{
		TenantID:         request.TenantID,
		TemplateKey:      request.TemplateKey,
		RequestedVersion: &version,
		Locale:           request.Locale,
		Variables:        request.Variables,
	})
	if err != nil {
		return ports.RenderedEmail{}, err
	}
	var variables verificationVariables
	if err := json.Unmarshal(resolved.Variables, &variables); err != nil {
		return ports.RenderedEmail{}, fmt.Errorf("%w: canonical variables could not be decoded", ports.ErrTemplateVariables)
	}
	purposeLabels := map[string]string{
		"REGISTER":       "注册",
		"RESET_PASSWORD": "重置密码",
		"LOGIN":          "登录",
	}
	view := verificationView{
		Code:            variables.Code,
		Purpose:         purposeLabels[variables.Purpose],
		ValidForSeconds: variables.ValidForSeconds,
	}
	var textBody, htmlBody strings.Builder
	if err := verificationTextTemplate.Execute(&textBody, view); err != nil {
		return ports.RenderedEmail{}, fmt.Errorf("render text template: %w", err)
	}
	if err := verificationHTMLTemplate.Execute(&htmlBody, view); err != nil {
		return ports.RenderedEmail{}, fmt.Errorf("render HTML template: %w", err)
	}
	rendered := ports.RenderedEmail{
		Subject:  "AI Nexus " + view.Purpose + "验证码",
		TextBody: textBody.String(),
		HTMLBody: htmlBody.String(),
	}
	if err := rendered.Validate(); err != nil {
		return ports.RenderedEmail{}, fmt.Errorf("render verification email: %w", err)
	}
	return rendered, nil
}

func validateVerificationVariables(raw json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var variables verificationVariables
	if err := decoder.Decode(&variables); err != nil {
		return nil, fmt.Errorf("%w: verification payload does not match schema", ports.ErrTemplateVariables)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: verification payload has trailing data", ports.ErrTemplateVariables)
	}
	validPurpose := variables.Purpose == "REGISTER" ||
		variables.Purpose == "RESET_PASSWORD" ||
		variables.Purpose == "LOGIN"
	if !sixDigits.MatchString(variables.Code) || !validPurpose ||
		variables.ValidForSeconds < 60 || variables.ValidForSeconds > 1800 {
		return nil, fmt.Errorf("%w: verification payload does not match schema", ports.ErrTemplateVariables)
	}
	canonical, err := json.Marshal(variables)
	if err != nil {
		return nil, fmt.Errorf("%w: canonicalize verification payload", ports.ErrTemplateVariables)
	}
	return canonical, nil
}
