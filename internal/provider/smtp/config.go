package smtp

import (
	"errors"
	"fmt"
	"net"
	"net/mail"
	"strconv"
	"strings"
	"time"
)

const ProviderKey = "smtp"

type SecurityMode string

const SecurityImplicitTLS SecurityMode = "implicit_tls"

type AuthMethod string

const (
	AuthLogin AuthMethod = "login"
	AuthPlain AuthMethod = "plain"
)

var ErrInvalidConfig = errors.New("invalid SMTP provider configuration")

type Config struct {
	Host           string
	Port           uint16
	Security       SecurityMode
	AuthMethod     AuthMethod
	Username       string
	AuthCode       string
	FromAddress    string
	FromName       string
	SessionTimeout time.Duration
}

func DefaultConfig() Config {
	return Config{
		Host:           "smtp.qq.com",
		Port:           465,
		Security:       SecurityImplicitTLS,
		AuthMethod:     AuthLogin,
		SessionTimeout: 10 * time.Second,
	}
}

func (c Config) Validate() error {
	if !validHost(c.Host) {
		return invalidConfig("host is invalid")
	}
	if c.Port == 0 {
		return invalidConfig("port is required")
	}
	if c.Security != SecurityImplicitTLS {
		return invalidConfig("security must be implicit_tls in this stage")
	}
	if c.AuthMethod != AuthLogin && c.AuthMethod != AuthPlain {
		return invalidConfig("auth method must be login or plain")
	}
	if !validCredentialField(c.Username, 320) {
		return invalidConfig("username is invalid")
	}
	if !validCredentialField(c.AuthCode, 1024) {
		return invalidConfig("authorization code is invalid")
	}
	if !validBareAddress(c.FromAddress) {
		return invalidConfig("from address must be one bare email address")
	}
	if strings.EqualFold(c.Host, "smtp.qq.com") && !strings.EqualFold(c.Username, c.FromAddress) {
		return invalidConfig("QQ SMTP from address must match the authentication username")
	}
	if len(c.FromName) > 256 || strings.ContainsAny(c.FromName, "\r\n") {
		return invalidConfig("from name is invalid")
	}
	if c.SessionTimeout < 100*time.Millisecond || c.SessionTimeout > 10*time.Minute {
		return invalidConfig("session timeout must be in range 100ms..10m")
	}
	return nil
}

func (c Config) Address() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(int(c.Port)))
}

func validHost(value string) bool {
	if value == "" || len(value) > 253 || strings.TrimSpace(value) != value ||
		strings.ContainsAny(value, "\r\n/:@") {
		return false
	}
	if net.ParseIP(value) != nil {
		return true
	}
	for _, label := range strings.Split(value, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') &&
				(character < 'A' || character > 'Z') &&
				(character < '0' || character > '9') && character != '-' {
				return false
			}
		}
	}
	return true
}

func validCredentialField(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\r\n\x00")
}

func validBareAddress(value string) bool {
	if value == "" || len(value) > 254 || strings.TrimSpace(value) != value ||
		strings.ContainsAny(value, "\r\n") {
		return false
	}
	parsed, err := mail.ParseAddress(value)
	return err == nil && parsed.Name == "" && parsed.Address == value
}

func invalidConfig(detail string) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, detail)
}
