//go:build integration

// Package postgrescontainer provides an isolated PostgreSQL instance for
// integration tests. It deliberately requires the integration build tag so
// ordinary unit tests never require Docker.
package postgrescontainer

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	postgresmodule "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const defaultImage = "postgres:18.4-alpine"

// Instance exposes both the database/sql handle used by Goose and the native
// pgx connection string used by repository tests.
type Instance struct {
	SQL              *sql.DB
	ConnectionString string
}

// Start preserves the compact migration-test API.
func Start(t *testing.T) *sql.DB {
	t.Helper()
	return StartInstance(t).SQL
}

// StartInstance creates a disposable PostgreSQL database and registers cleanup
// with t.
func StartInstance(t *testing.T) Instance {
	t.Helper()

	// The first run may need to pull the pinned PostgreSQL image. Keep this
	// budget separate from the much shorter database connection timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	image := os.Getenv("TEST_POSTGRES_IMAGE")
	if image == "" {
		image = defaultImage
	}

	container, err := postgresmodule.Run(
		ctx,
		image,
		postgresmodule.WithDatabase("email_service_test"),
		postgresmodule.WithUsername("email_service_test"),
		postgresmodule.WithPassword("email_service_test"),
		postgresmodule.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start PostgreSQL container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)

	connectionString, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("build PostgreSQL connection string: %v", err)
	}

	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		t.Fatalf("open PostgreSQL connection: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close PostgreSQL connection: %v", err)
		}
	})

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pingCancel()
	if err := db.PingContext(pingCtx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	return Instance{SQL: db, ConnectionString: connectionString}
}
