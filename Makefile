.PHONY: bootstrap proto lint sqlc test test-int cover-auth

bootstrap:
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

proto:
	cd contracts/proto && buf dep update && buf generate
	cd gen/go && go mod tidy

lint:
	cd contracts/proto && buf lint
	@for d in $$(go list -f '{{.Dir}}' -m); do \
		echo "golangci-lint $$d"; (cd $$d && golangci-lint run ./...) || exit 1; \
	done

sqlc:
	MSYS_NO_PATHCONV=1 docker run --rm -v "$(CURDIR)/services/auth:/src" -w /src sqlc/sqlc:1.29.0 generate

test:
	@for d in $$(go list -f '{{.Dir}}' -m); do \
		echo "go test $$d"; (cd $$d && go test ./...) || exit 1; \
	done

test-int:
	@for d in services/*/; do \
		echo "go test -tags integration $$d"; (cd $$d && go test -count=1 -tags integration ./...) || exit 1; \
	done

cover-auth:
	cd services/auth && go test -coverprofile=coverage.out ./internal/domain/... ./internal/app/... && \
		go tool cover -func=coverage.out | awk '/^total:/ { sub("%","",$$3); printf "total coverage: %s%%\n", $$3; if ($$3+0 < 70) { print "coverage below 70%"; exit 1 } }'
