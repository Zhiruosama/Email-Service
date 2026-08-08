GO ?= go
PROTOC ?= protoc
BUF_VERSION ?= v1.72.0
GOOSE_VERSION ?= v3.27.3
POSTGRES_IMAGE ?= postgres:18.4-alpine
DATABASE_URL ?= postgres://email_service:email_service_dev@localhost:5432/email_service?sslmode=disable
GOOSE := $(GO) run -tags=no_clickhouse,no_libsql,no_mssql,no_mysql,no_sqlite3,no_vertica,no_ydb github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION)
PROTO_DIR := api/proto
GEN_DIR := gen/go
PROTO_FILES := $(shell find $(PROTO_DIR) -type f -name '*.proto' | sort)
PROTOC_GEN_GO := $(shell $(GO) tool -n protoc-gen-go)
PROTOC_GEN_GO_GRPC := $(shell $(GO) tool -n protoc-gen-go-grpc)

.PHONY: help generate buf-generate proto-check proto-format-check proto-lint format test test-integration check check-all db-up db-down db-status migrate-up migrate-down migrate-status migrate-validate

help:
	@echo "generate     Generate Go protobuf and gRPC bindings"
	@echo "buf-generate Generate bindings through pinned Buf"
	@echo "proto-check  Compile protobuf schemas without generating code"
	@echo "proto-format-check Check protobuf formatting with pinned Buf"
	@echo "proto-lint   Run pinned Buf lint"
	@echo "format       Format Go source files"
	@echo "test         Run Go tests"
	@echo "test-integration Run PostgreSQL integration tests through Testcontainers"
	@echo "check        Run schema compilation, generation, formatting, and tests"
	@echo "check-all    Run Buf lint followed by check"
	@echo "db-up        Start the local PostgreSQL container and wait until healthy"
	@echo "db-down      Stop local containers without deleting database volumes"
	@echo "migrate-up   Apply all PostgreSQL migrations"
	@echo "migrate-down Roll back one PostgreSQL migration"
	@echo "migrate-status Show PostgreSQL migration status"
	@echo "migrate-validate Validate migration file ordering and annotations"

generate:
	@mkdir -p $(GEN_DIR)
	$(PROTOC) \
		-I $(PROTO_DIR) \
		-I /usr/include \
		--plugin=protoc-gen-go=$(PROTOC_GEN_GO) \
		--plugin=protoc-gen-go-grpc=$(PROTOC_GEN_GO_GRPC) \
		--go_out=$(GEN_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

proto-check:
	@descriptor_file=$$(mktemp); \
	trap 'rm -f "$$descriptor_file"' EXIT; \
	$(PROTOC) \
		-I $(PROTO_DIR) \
		-I /usr/include \
		--include_imports \
		--descriptor_set_out="$$descriptor_file" \
		$(PROTO_FILES)

buf-generate:
	$(GO) run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) generate

proto-lint:
	$(GO) run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) lint

proto-format-check:
	$(GO) run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) format --diff --exit-code

format:
	GOFLAGS=-tags=integration $(GO) fmt ./...

test:
	$(GO) test ./...

test-integration:
	TEST_POSTGRES_IMAGE=$(POSTGRES_IMAGE) $(GO) test -tags=integration ./internal/integration/...

db-up:
	POSTGRES_IMAGE=$(POSTGRES_IMAGE) MAIL_POSTGRES_PORT=$${MAIL_POSTGRES_PORT:-5432} docker compose up -d --wait postgres

db-down:
	docker compose down

db-status:
	docker compose ps

migrate-up:
	$(GOOSE) -dir db/migrations/sql postgres "$(DATABASE_URL)" up

migrate-down:
	$(GOOSE) -dir db/migrations/sql postgres "$(DATABASE_URL)" down

migrate-status:
	$(GOOSE) -dir db/migrations/sql postgres "$(DATABASE_URL)" status

migrate-validate:
	$(GOOSE) -dir db/migrations/sql validate

check: proto-check generate format test

check-all: proto-format-check proto-lint check
