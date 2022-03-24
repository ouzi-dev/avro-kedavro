
BUILD_PATH = github.com/ouzi-dev/avro-kedavro

HAS_GOLANCI_LINT := $(shell command -v golangci-lint;)
GOLANGCI_LINT_VERSION := v1.21.0

SRC = $(shell find . -type f -name '*.go' -not -path "./vendor/*")

.PHONY: tidy
tidy:
	@echo "tidy target..."
	@go mod tidy

.PHONY: vendor
vendor: tidy
	@echo "vendor target..."
	@go mod vendor

.PHONY: build
build: fmt 
	@echo "build target..."
	@go build ./...

.PHONY: test
test: fmt 
	@echo "test target..."
	@go test ./... -v -count=1

.PHONY: lint
lint: bootstrap 
	@echo "lint target..."
	@golangci-lint run --enable-all --disable lll,godox,wsl,funlen ./...

.PHONY: bootstrap
bootstrap:
	@echo "bootstrap target..."
ifndef HAS_GOLANCI_LINT
	@GOPROXY=direct GOSUMDB=off go get -u github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
endif

.PHONY: fmt
fmt:
	@echo "fmt target..."
	@gofmt -l -w -s $(SRC)

.PHONE: covhtml
covhtml: test
	@scripts/coverage.sh
	@go tool cover -html=.cover/cover.out

.PHONY: test-checker-cli
test-checker-cli:
	$(MAKE) -C cli test

.PHONY: package-checker-cli
package-checker-cli:
	$(MAKE) -C cli clean dist VERSION=$(VERSION)

.PHONY: semantic-release
semantic-release:
	npm ci
	npx semantic-release

.PHONY: semantic-release-dry-run
semantic-release-dry-run:
	npm ci
	npx semantic-release -d

.PHONY: install-npm-check-updates
install-npm-check-updates:
	npm install npm-check-updates

.PHONY: update-npm-dependencies
update-npm-dependencies: install-npm-check-updates
	ncu -u
	npm install