# Netsy <https://netsy.dev>
# Copyright The Netsy Authors
# SPDX-License-Identifier: Apache-2.0

.DEFAULT_GOAL := help

BINDIR=bin

BINARY_NAME=netsy
MAIN_PKG=./cmd/netsy

BUILDVARS_PKG=github.com/netsy-dev/netsy/internal/buildvars

CURRENT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

VERSION_TAG=$(shell if git diff --quiet && git diff --cached --quiet; then git describe --tags --exact-match 2>/dev/null; fi)
BUILD_VERSION=$(if $(VERSION_TAG),$(VERSION_TAG),dev)
BUILD_DATE=$(shell date -u '+%Y-%m-%dT%H:%M:%S')
COMMIT_HASH=$(shell git rev-parse --short HEAD)
COMMIT_DATE=$(shell git log -1 --format=%cd --date=format:'%Y-%m-%dT%H:%M:%S')
COMMIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

# Cross-compilation settings, defaulting OS/ARCH to the current platform
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
CGO_ENABLED=1
EXTRA_LD_FLAGS=
CC ?= cc
CXX ?= c++
ifeq ($(GOOS),linux)
	BUILD_TAGS=linux
	EXTRA_LD_FLAGS=-extldflags -static
else ifeq ($(GOOS),darwin)
	ifeq ($(GOARCH),amd64)
		BUILD_TAGS=darwin amd64
		CC=clang
		CXX=clang++
	else ifeq ($(GOARCH),arm64)
		BUILD_TAGS=darwin arm64
		CC=clang
		CXX=clang++
	endif
endif

# Number of dev instances
NETSY_COUNT ?= 1

.PHONY: help setup fmt lint precommit test build proto clean start restart stop status tail attach image

help: ## Show available targets
	@echo "Usage: make <target>"
	@awk 'BEGIN {FS = ":.*?## "} /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5)} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Verify required tools and enable git hooks
	@command -v go >/dev/null 2>&1 || { echo "go is required but not installed"; exit 1; }
	@command -v air >/dev/null 2>&1 || { echo "air is required but not installed (go install github.com/air-verse/air@latest)"; exit 1; }
	@command -v overmind >/dev/null 2>&1 || { echo "overmind is required but not installed (brew install overmind)"; exit 1; }
	@command -v shellcheck >/dev/null 2>&1 || { echo "shellcheck is required but not installed"; exit 1; }
	@go tool golangci-lint version >/dev/null 2>&1 || { echo "golangci-lint is required (run 'go get -tool github.com/golangci/golangci-lint/cmd/golangci-lint@latest')"; exit 1; }
	@echo "All required tools are installed."
	@cp scripts/git-hooks/pre-commit .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@cp scripts/git-hooks/commit-msg .git/hooks/commit-msg
	@chmod +x .git/hooks/commit-msg
	@echo "Git hooks installed."

##@ Build & Test

fmt: ## Format Go source files
	@go fmt ./...

lint: ## Run linters (Go + shellcheck)
	@go tool golangci-lint run --timeout=5m
	@echo "Running shellcheck..."
	@shellcheck scripts/**/*.sh

precommit: ## Check formatting and run linters (read-only)
	@echo "Checking formatting..."
	@UNFORMATTED=$$(gofmt -l . 2>&1); \
	if [ -n "$$UNFORMATTED" ]; then \
		echo "The following files need formatting (run 'make fmt'):"; \
		echo "$$UNFORMATTED"; \
		exit 1; \
	fi
	@$(MAKE) lint

test: ## Run tests with race detector
	go test -v -race ./...

build: ## Build the netsy binary
	mkdir -p $(BINDIR)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
	CGO_ENABLED=$(CGO_ENABLED) CC=$(CC) CXX=$(CXX) \
	go build $(if $(BUILD_TAGS),-tags "$(BUILD_TAGS)") \
	    -o $(BINDIR)/$(BINARY_NAME) \
		-trimpath \
		-ldflags "$(EXTRA_LD_FLAGS) \
		-X $(BUILDVARS_PKG).buildVersion=$(BUILD_VERSION) \
		-X $(BUILDVARS_PKG).buildDate=$(BUILD_DATE) \
		-X $(BUILDVARS_PKG).commitHash=$(COMMIT_HASH) \
		-X $(BUILDVARS_PKG).commitDate=$(COMMIT_DATE) \
		-X $(BUILDVARS_PKG).commitBranch=$(COMMIT_BRANCH) \
		" $(MAIN_PKG)
	printf "%s" "$(BUILD_VERSION)-$(COMMIT_HASH)" > $(BINDIR)/version.txt

proto: ## Generate Go files from protobuf definitions
	protoc -I=$(CURRENT) \
	       --go_out=$(CURRENT)internal \
	       --go_opt=paths=source_relative \
	       --go-grpc_out=$(CURRENT)internal \
	       --go-grpc_opt=paths=source_relative $(CURRENT)proto/*.proto

clean: stop ## Force-stop dev environment and remove build artifacts and temp files
	rm -rf $(BINDIR) temp/

image: ## Build container image
	docker build -f images/netsy/Containerfile -t ghcr.io/netsy-dev/netsy:latest .

##@ Dev Environment
define archive-logs
	@if ls temp/logs/*.log 1>/dev/null 2>&1; then \
		archive="temp/logs/previous/$$(date +%Y%m%d-%H%M%S)"; \
		mkdir -p "$$archive"; \
		mv temp/logs/*.log "$$archive/"; \
		echo "Previous logs archived to $$archive"; \
	fi
endef

start: ## Start development environment (NETSY_COUNT=1 by default)
	@./scripts/dev/check-ports.sh $(NETSY_COUNT)
	@test -f temp/certs/ca.crt || ./scripts/dev/certs.sh $(NETSY_COUNT)
	@if [ "$(NETSY_COUNT)" -gt 1 ] || [ "$(NETSY_DEV_AIR)" = "0" ]; then $(MAKE) build; fi
	$(archive-logs)
	OVERMIND_FORMATION=s3=1,netsy=$(NETSY_COUNT) overmind start -D
	@echo "Development environment started. Use 'make tail' to view logs."

restart: ## Restart all Netsy instances (use after 'make build')
	$(archive-logs)
	@overmind restart netsy

stop: ## Force-stop development environment
	@overmind kill 2>/dev/null || overmind quit 2>/dev/null || true
	@rm -f .overmind.sock
	@echo "Development environment stopped."

status: ## Show status of dev processes and ports
	@./scripts/dev/check-ports.sh $(NETSY_COUNT)
	@echo ""
	@echo "Overmind processes:"
	@overmind ps 2>/dev/null || echo "  No overmind session running."

tail: ## Tail all dev log output (coloured by process)
	@bash -c 'overmind echo > >(grep --line-buffered -v "^system | Echo #") & pid=$$!; trap "kill $$pid 2>/dev/null; exit 0" INT TERM; wait $$pid'

attach: ## Attach to overmind tmux session
	@overmind connect
