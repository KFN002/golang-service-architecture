# ============================================================================
# perfect-go-service — developer entrypoint.
# Layout: backend/ (Go module, build/ holds its tool configs)
#         frontend/ (Next.js) · deploy/ (compose, Dockerfiles, config/ infra confs)
#         .github/workflows (CI, fixed path)
# CI runs these exact targets; `make help` lists everything.
# ============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Pinned tool versions — no global installs, `go run` resolves and caches.
SQLC    := go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0
BUF     := go run github.com/bufbuild/buf/cmd/buf@v1.57.2
GOOSE   := go run github.com/pressly/goose/v3/cmd/goose@v3.26.0
LINT    := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.6.2
VULN    := go run golang.org/x/vuln/cmd/govulncheck@latest

PG_DSN       ?= postgres://calc:calcpass@localhost:5432/calc?sslmode=disable
AUDIT_PG_DSN ?= postgres://audit:auditpass@localhost:5433/audit?sslmode=disable

COMPOSE := docker compose -f deploy/docker-compose.yml

## ---- code generation -------------------------------------------------------

.PHONY: proto-gen
proto-gen: ## Regenerate gRPC/gateway/vtproto/OpenAPI code from protos
	@mkdir -p backend/bin
	@cd backend && GOBIN=$(PWD)/backend/bin go install \
		google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
	@cd backend && GOBIN=$(PWD)/backend/bin go install \
		google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
	@cd backend && GOBIN=$(PWD)/backend/bin go install \
		github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2.29.0 \
		github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v2.29.0
	@cd backend && GOBIN=$(PWD)/backend/bin go install \
		github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@v0.6.0
	cd backend && $(BUF) lint --config build/buf.yaml
	cd backend && $(BUF) generate --config build/buf.yaml --template build/buf.gen.yaml

.PHONY: sqlc-gen
sqlc-gen: ## Regenerate typed queries for both databases
	$(SQLC) -f backend/build/sqlc.yaml generate

.PHONY: generate
generate: proto-gen sqlc-gen ## Regenerate everything

.PHONY: generate-check
generate-check: generate ## CI drift check: fail if generated code was stale
	@git diff --exit-code --stat backend/gen/ backend/internal/repo/persistent/sqlcgen/ backend/internal/repo/auditstore/sqlcgen/ \
		|| (echo "✖ generated code is stale — run 'make generate' and commit" && exit 1)

## ---- database --------------------------------------------------------------

.PHONY: migrate-up
migrate-up: ## Apply main DB migrations
	$(GOOSE) -dir backend/db/main/migrations postgres "$(PG_DSN)" up

.PHONY: migrate-down
migrate-down: ## Roll back one main DB migration
	$(GOOSE) -dir backend/db/main/migrations postgres "$(PG_DSN)" down

.PHONY: migrate-status
migrate-status: ## Migration status, main DB
	$(GOOSE) -dir backend/db/main/migrations postgres "$(PG_DSN)" status

.PHONY: audit-migrate-up
audit-migrate-up: ## Apply audit DB migrations
	$(GOOSE) -dir backend/db/audit/migrations postgres "$(AUDIT_PG_DSN)" up

.PHONY: audit-migrate-status
audit-migrate-status: ## Migration status, audit DB
	$(GOOSE) -dir backend/db/audit/migrations postgres "$(AUDIT_PG_DSN)" status

## ---- quality ---------------------------------------------------------------

.PHONY: fmt
fmt: ## Format Go code
	cd backend && gofmt -w $$(find . -name '*.go' -not -path './gen/*' -not -path '*/sqlcgen/*')

.PHONY: vet
vet: ## go vet everything
	cd backend && go vet ./...

.PHONY: lint
lint: ## golangci-lint, ALL linters (includes gosec)
	cd backend && $(LINT) run --config build/golangci.yml ./...

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	cd backend && $(VULN) ./...

.PHONY: test
test: ## Unit tests with the race detector + coverage
	cd backend && go test -race -coverprofile=coverage.out ./pkg/... ./internal/... ./config/...

.PHONY: test-integration
test-integration: ## Container-backed integration suite (needs Docker)
	cd backend && go test -tags integration -race -timeout 20m ./integration/...

.PHONY: test-all
test-all: test test-integration ## Everything

## ---- run -------------------------------------------------------------------

.PHONY: up
up: ## Build and start the full stack
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop the stack (volumes preserved)
	$(COMPOSE) down

.PHONY: logs
logs: ## Tail all service logs
	$(COMPOSE) logs -f --tail=100

.PHONY: up-infra
up-infra: ## Start only the data/telemetry planes (local binary debugging)
	$(COMPOSE) up -d postgres-main postgres-audit redis-main redis-audit rabbitmq otel-collector jaeger prometheus grafana

.PHONY: run-orchestrator
run-orchestrator: ## Run the orchestrator locally against up-infra
	cd backend && go run ./cmd/orchestrator

.PHONY: run-agent
run-agent: ## Run one agent locally against up-infra
	cd backend && go run ./cmd/agent

.PHONY: run-audit
run-audit: ## Run the audit service locally against up-infra
	cd backend && PG_DSN="$(AUDIT_PG_DSN)" REDIS_ADDR=localhost:6380 go run ./cmd/audit

.PHONY: web-dev
web-dev: ## Next.js dev server with API proxying
	cd frontend && npm run dev

## ---- misc ------------------------------------------------------------------

.PHONY: build
build: ## Compile all binaries
	cd backend && go build ./...

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf backend/bin backend/coverage.out frontend/.next

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
