.PHONY: bootstrap proto proto-deps openapi lint sqlc test test-int cover-auth cover-job \
	infra-up obs-up up down migrate seed smoke logs

# Compose stack (deploy/docker), project name smarthire. Paths are absolute
# and MSYS_NO_PATHCONV stops Git Bash on Windows from rewriting them.
COMPOSE_DIR := $(CURDIR)/deploy/docker
COMPOSE := MSYS_NO_PATHCONV=1 docker compose -p smarthire \
	--project-directory $(COMPOSE_DIR) --env-file $(CURDIR)/.env \
	-f $(COMPOSE_DIR)/docker-compose.infra.yml \
	-f $(COMPOSE_DIR)/docker-compose.obs.yml \
	-f $(COMPOSE_DIR)/docker-compose.yml
GATEWAY_URL ?= http://localhost:8080
SMOKE_ORIGIN ?= http://localhost:3001
# Redpanda dev listener; an IPv4 literal because the port is published on
# 127.0.0.1 only and localhost may resolve to ::1.
KAFKA_ADDR ?= 127.0.0.1:19092

bootstrap:
	go install github.com/bufbuild/buf/cmd/buf@v1.75.0
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
	go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.20.1
	go run ./tools/cmd/keygen -out deploy/docker/secrets/jwt.pem
	@test -f .env || { cp .env.example .env && echo "created .env from .env.example"; }

# Deliberately bumps buf deps (rewrites buf.lock); run, then `make proto`, and commit both.
proto-deps:
	cd contracts/proto && buf dep update

proto:
	cd contracts/proto && buf generate
	cd gen/go && go mod tidy
	$(MAKE) openapi

openapi:
	go run ./tools/cmd/openapimerge -gen contracts/openapi/gen/gateway.swagger.json -hand contracts/openapi/gateway.handwritten.yaml -out contracts/openapi/gateway.yaml

lint:
	cd contracts/proto && buf lint
	@for d in $$(go list -f '{{.Dir}}' -m); do \
		echo "golangci-lint $$d"; (cd $$d && golangci-lint run ./...) || exit 1; \
	done

sqlc:
	@for s in auth job; do \
		MSYS_NO_PATHCONV=1 docker run --rm -v "$(CURDIR)/services/$$s:/src" -w /src sqlc/sqlc:1.29.0 generate || exit 1; \
	done

test:
	@for d in $$(go list -f '{{.Dir}}' -m); do \
		echo "go test $$d"; (cd $$d && go test ./...) || exit 1; \
	done

test-int:
	@for d in pkg services/*/; do \
		echo "go test -tags integration $$d"; (cd $$d && go test -count=1 -tags integration ./...) || exit 1; \
	done

cover-auth:
	cd services/auth && go test -coverprofile=coverage.out ./internal/domain/... ./internal/app/... && \
		go tool cover -func=coverage.out | awk '/^total:/ { sub("%","",$$3); printf "total coverage: %s%%\n", $$3; if ($$3+0 < 70) { print "coverage below 70%"; exit 1 } }'

cover-job:
	cd services/job && go test -coverprofile=coverage.out ./internal/domain/... ./internal/app/... && \
		go tool cover -func=coverage.out | awk '/^total:/ { sub("%","",$$3); printf "total coverage: %s%%\n", $$3; if ($$3+0 < 70) { print "coverage below 70%"; exit 1 } }'

infra-up:
	$(COMPOSE) up -d --wait postgres redis redpanda redpanda-console
	$(COMPOSE) run --rm topics-init

obs-up:
	$(COMPOSE) up -d otel-collector tempo grafana

# Builds and starts everything, then waits until the gateway listens (it only
# starts serving once it has fetched the auth JWKS) and until a job RPC
# succeeds through it (job's /readyz answers before its gRPC server listens).
up:
	$(COMPOSE) up -d --build
	@echo "waiting for gateway at $(GATEWAY_URL)"; \
	for i in $$(seq 1 60); do \
		curl -fsS -o /dev/null $(GATEWAY_URL)/healthz && { echo "gateway is up"; exit 0; }; \
		sleep 1; \
	done; \
	echo "gateway did not become healthy"; $(COMPOSE) logs --tail=50 auth gateway; exit 1
	@echo "waiting for job service at $(GATEWAY_URL)/v1/jobs"; \
	for i in $$(seq 1 60); do \
		curl -fsS -o /dev/null $(GATEWAY_URL)/v1/jobs && { echo "job service is up"; exit 0; }; \
		sleep 1; \
	done; \
	echo "job service did not become reachable"; $(COMPOSE) logs --tail=50 job gateway; exit 1

down:
	$(COMPOSE) --profile tools down

# Re-runs the auth and job migrations (the one-shot *-migrate containers).
migrate:
	$(COMPOSE) run --rm auth-migrate && $(COMPOSE) run --rm job-migrate

seed:
	$(COMPOSE) run --rm auth-seed

smoke:
	go run ./tools/cmd/smoke -base $(GATEWAY_URL) -origin $(SMOKE_ORIGIN) -kafka $(KAFKA_ADDR)

logs:
	$(COMPOSE) logs -f --tail=100
