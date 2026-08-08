// Package migrations exposes immutable SQL migrations to integration tests.
// Production deployments run the same files as a separate migration step;
// application replicas do not migrate the database during startup.
package migrations

import "embed"

// Files contains all versioned Goose SQL migrations under the sql directory.
//
//go:embed sql/*.sql
var Files embed.FS
