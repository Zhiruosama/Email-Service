//go:build integration

// Package rabbitmqcontainer provides an isolated RabbitMQ broker for
// integration tests. Ordinary unit tests never require Docker.
package rabbitmqcontainer

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultImage    = "rabbitmq:4.3.4-management-alpine"
	defaultUsername = "email_service_test"
	defaultPassword = "email_service_test"
)

type Instance struct {
	Container testcontainers.Container
	URL       string
}

func Start(t *testing.T) Instance {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	image := os.Getenv("TEST_RABBITMQ_IMAGE")
	if image == "" {
		image = defaultImage
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: image,
			Env: map[string]string{
				"RABBITMQ_DEFAULT_USER": defaultUsername,
				"RABBITMQ_DEFAULT_PASS": defaultPassword,
			},
			ExposedPorts: []string{"5672/tcp"},
			WaitingFor: wait.ForListeningPort("5672/tcp").
				WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start RabbitMQ container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("resolve RabbitMQ host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatalf("resolve RabbitMQ port: %v", err)
	}
	brokerURL := (&url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(defaultUsername, defaultPassword),
		Host:   fmt.Sprintf("%s:%s", host, port.Port()),
		Path:   "/",
	}).String()

	return Instance{Container: container, URL: brokerURL}
}
