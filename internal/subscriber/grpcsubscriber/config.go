package grpcsubscriber

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidConfig = errors.New("invalid gRPC subscriber configuration")

type Config struct {
	Address string
}

func (c Config) Validate() error {
	if c.Address == "" || len(c.Address) > 512 || strings.TrimSpace(c.Address) != c.Address ||
		strings.ContainsAny(c.Address, "\r\n") {
		return fmt.Errorf(
			"%w: address must contain 1..512 bytes without surrounding whitespace or newlines",
			ErrInvalidConfig,
		)
	}
	return nil
}
