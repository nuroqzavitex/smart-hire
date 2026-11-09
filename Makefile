.PHONY: bootstrap proto lint

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
