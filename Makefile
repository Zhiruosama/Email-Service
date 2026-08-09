GO ?= go
PROTOC ?= protoc
BUF_VERSION ?= v1.72.0
GOOSE_VERSION ?= v3.27.3
POSTGRES_IMAGE ?= postgres:18.4-alpine
RABBITMQ_IMAGE ?= rabbitmq:4.3.4-management-alpine
DATABASE_URL ?= postgres://email_service:email_service_dev@localhost:5432/email_service?sslmode=disable
RABBITMQ_URL ?= amqp://email_service:email_service_dev@localhost:5672/
GOOSE := $(GO) run -tags=no_clickhouse,no_libsql,no_mssql,no_mysql,no_sqlite3,no_vertica,no_ydb github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION)
PROTO_DIR := api/proto
GEN_DIR := gen/go
PROTO_FILES := $(shell find $(PROTO_DIR) -type f -name '*.proto' | sort)
PROTOC_GEN_GO := $(shell $(GO) tool -n protoc-gen-go)
PROTOC_GEN_GO_GRPC := $(shell $(GO) tool -n protoc-gen-go-grpc)

.PHONY: help build run generate buf-generate proto-check proto-format-check proto-lint format test test-integration test-smtp-real check check-all infra-up infra-down infra-status db-up db-down db-status db-dev-seed mq-up mq-down mq-status mq-policy-apply mq-policy-status migrate-up migrate-down migrate-status migrate-validate

help:
	@echo "build        Build the mail-service binary"
	@echo "run          Run mail-service using the exported environment"
	@echo "generate     Generate Go protobuf and gRPC bindings"
	@echo "buf-generate Generate bindings through pinned Buf"
	@echo "proto-check  Compile protobuf schemas without generating code"
	@echo "proto-format-check Check protobuf formatting with pinned Buf"
	@echo "proto-lint   Run pinned Buf lint"
	@echo "format       Format Go source files"
	@echo "test         Run Go tests"
	@echo "test-integration Run PostgreSQL and RabbitMQ integration tests through Testcontainers"
	@echo "test-smtp-real Send one explicitly authorized real SMTP smoke-test email"
	@echo "check        Run schema compilation, generation, formatting, and tests"
	@echo "check-all    Run Buf lint followed by check"
	@echo "infra-up     Start PostgreSQL and RabbitMQ and wait until healthy"
	@echo "infra-down   Stop local containers without deleting data volumes"
	@echo "infra-status Show local container status"
	@echo "db-up        Start the local PostgreSQL container and wait until healthy"
	@echo "db-down      Stop PostgreSQL without deleting its data volume"
	@echo "db-status    Show PostgreSQL status"
	@echo "db-dev-seed Seed the fixed local development tenant"
	@echo "mq-up        Start the local RabbitMQ container and wait until healthy"
	@echo "mq-down      Stop RabbitMQ without deleting its data volume"
	@echo "mq-status    Show RabbitMQ status"
	@echo "mq-policy-apply Apply dispatch and lifecycle retry/dead-letter policies"
	@echo "mq-policy-status Show RabbitMQ policies"
	@echo "migrate-up   Apply all PostgreSQL migrations"
	@echo "migrate-down Roll back one PostgreSQL migration"
	@echo "migrate-status Show PostgreSQL migration status"
	@echo "migrate-validate Validate migration file ordering and annotations"

build:
	@mkdir -p bin
	$(GO) build -o bin/mail-service ./cmd/mail-service

run:
	$(GO) run ./cmd/mail-service

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
	TEST_POSTGRES_IMAGE=$(POSTGRES_IMAGE) TEST_RABBITMQ_IMAGE=$(RABBITMQ_IMAGE) $(GO) test -tags=integration ./internal/integration/...

test-smtp-real:
	@if [ "$${MAIL_SMTP_REAL_TEST_ENABLED:-false}" != "true" ]; then \
		echo "refusing real SMTP test: set MAIL_SMTP_REAL_TEST_ENABLED=true explicitly"; \
		exit 1; \
	fi
	$(GO) test -tags=real_smtp ./internal/provider/smtp -run '^TestRealQQSMTP$$' -count=1

infra-up:
	POSTGRES_IMAGE=$(POSTGRES_IMAGE) RABBITMQ_IMAGE=$(RABBITMQ_IMAGE) \
		MAIL_POSTGRES_PORT=$${MAIL_POSTGRES_PORT:-5432} \
		MAIL_RABBITMQ_PORT=$${MAIL_RABBITMQ_PORT:-5672} \
		MAIL_RABBITMQ_MANAGEMENT_PORT=$${MAIL_RABBITMQ_MANAGEMENT_PORT:-15672} \
		docker compose up -d --wait postgres rabbitmq
	$(MAKE) mq-policy-apply

infra-down:
	docker compose down

infra-status:
	docker compose ps

db-up:
	POSTGRES_IMAGE=$(POSTGRES_IMAGE) MAIL_POSTGRES_PORT=$${MAIL_POSTGRES_PORT:-5432} docker compose up -d --wait postgres

db-down:
	docker compose stop postgres

db-status:
	docker compose ps postgres

db-dev-seed:
	docker compose exec -T postgres psql \
		--username email_service \
		--dbname email_service \
		--set ON_ERROR_STOP=1 \
		< db/seeds/development.sql

mq-up:
	RABBITMQ_IMAGE=$(RABBITMQ_IMAGE) \
		MAIL_RABBITMQ_PORT=$${MAIL_RABBITMQ_PORT:-5672} \
		MAIL_RABBITMQ_MANAGEMENT_PORT=$${MAIL_RABBITMQ_MANAGEMENT_PORT:-15672} \
		docker compose up -d --wait rabbitmq
	$(MAKE) mq-policy-apply

mq-down:
	docker compose stop rabbitmq

mq-status:
	docker compose ps rabbitmq

mq-policy-apply:
	docker compose exec -T rabbitmq rabbitmqctl set_policy \
		--vhost / \
		mail-dispatch-reliability \
		'^mail[.]dispatch[.]v1[.]q$$' \
		'{"dead-letter-exchange":"mail.dead.v1","dead-letter-routing-key":"mail.dispatch.dead.v1","dead-letter-strategy":"at-least-once","overflow":"reject-publish","delivery-limit":20,"delayed-retry-type":"failed","delayed-retry-min":1000,"delayed-retry-max":30000}' \
		--priority 100 \
		--apply-to quorum_queues
	docker compose exec -T rabbitmq rabbitmqctl set_policy \
		--vhost / \
		mail-lifecycle-reliability \
		'^mail[.]lifecycle[.]v1[.]q$$' \
		'{"dead-letter-exchange":"mail.dead.v1","dead-letter-routing-key":"mail.lifecycle.dead.v1","dead-letter-strategy":"at-least-once","overflow":"reject-publish","delivery-limit":20,"delayed-retry-type":"failed","delayed-retry-min":1000,"delayed-retry-max":30000}' \
		--priority 100 \
		--apply-to quorum_queues

mq-policy-status:
	docker compose exec -T rabbitmq rabbitmqctl list_policies --vhost /

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
