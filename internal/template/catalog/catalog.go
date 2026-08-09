// Package catalog provides the first immutable in-process template catalog.
// A database-backed control-plane adapter can replace it through the same port.
package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"

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
