GO ?= go
PROTOC ?= protoc
BUF_VERSION ?= v1.72.0
PROTO_DIR := api/proto
GEN_DIR := gen/go
PROTO_FILES := $(shell find $(PROTO_DIR) -type f -name '*.proto' | sort)
PROTOC_GEN_GO := $(shell $(GO) tool -n protoc-gen-go)
PROTOC_GEN_GO_GRPC := $(shell $(GO) tool -n protoc-gen-go-grpc)

.PHONY: help generate buf-generate proto-check proto-format-check proto-lint format test check check-all

help:
	@echo "generate     Generate Go protobuf and gRPC bindings"
	@echo "buf-generate Generate bindings through pinned Buf"
	@echo "proto-check  Compile protobuf schemas without generating code"
	@echo "proto-format-check Check protobuf formatting with pinned Buf"
	@echo "proto-lint   Run pinned Buf lint"
	@echo "format       Format Go source files"
	@echo "test         Run Go tests"
	@echo "check        Run schema compilation, generation, formatting, and tests"
	@echo "check-all    Run Buf lint followed by check"

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
	$(GO) fmt ./...

test:
	$(GO) test ./...

check: proto-check generate format test

check-all: proto-format-check proto-lint check
