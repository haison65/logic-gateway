PROTOC ?= protoc
PROTO_PATH := proto
GEN_DIR := proto/gen/go
PROTO_FILES := \
	$(PROTO_PATH)/common.proto \
	$(PROTO_PATH)/register.proto \
	$(PROTO_PATH)/heartbeat.proto \
	$(PROTO_PATH)/data.proto \
	$(PROTO_PATH)/error.proto \
	$(PROTO_PATH)/envelope.proto
PROTOC_GEN_GO_VERSION := v1.36.12

.PHONY: build test vet race proto proto-tools clean

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

race:
	go test -race ./...

proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

proto: proto-tools
	mkdir -p $(GEN_DIR)
	$(PROTOC) \
		--proto_path=$(PROTO_PATH) \
		--go_out=$(GEN_DIR) \
		--go_opt=paths=source_relative \
		$(PROTO_FILES)

clean:
	go clean ./...
