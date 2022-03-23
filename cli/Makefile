BUILD_PATH = github.com/ouzi-dev/avro-kedavro/cli
TARGETS   ?= darwin/amd64 linux/amd64 windows/amd64
DIST_DIRS = find * -type d -exec
LDFLAGS   := -w -s
NAME = avro-kedavro-checker

GIT_SHORT_COMMIT := $(shell git rev-parse --short HEAD)
GIT_TAG    := $(shell git describe --tags --abbrev=0 --exact-match 2>/dev/null)
GIT_DIRTY  = $(shell test -n "`git status --porcelain`" && echo "dirty" || echo "clean")

TMP_VERSION := $(GIT_SHORT_COMMIT)
HAS_GOX := $(shell command -v gox;)

ifndef VERSION
ifeq ($(GIT_DIRTY), clean)
ifdef GIT_TAG
	TMP_VERSION = $(GIT_TAG)
endif
endif
endif

VERSION ?= $(TMP_VERSION)

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

.PHONY: test
test: fmt 
	@echo "test target..."
	@go test ./... -v -count=1

.PHONY: lint
lint: bootstrap-lint
	@echo "lint target..."
	@golangci-lint run --enable-all --disable lll,godox,wsl,funlen,gochecknoglobals,gochecknoinits ./...

.PHONY: bootstrap-lint
bootstrap-lint:
	@echo "bootstrap-lint target..."
ifndef HAS_GOLANCI_LINT
	@GOPROXY=direct GOSUMDB=off go get -u github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
endif

.PHONY: fmt
fmt:
	@echo "fmt target..."
	@find . -name '*.go' -not -wholename './vendor/*' | while read -r file; do gofmt -w -s "$$file"; goimports -w "$$file"; done

.PHONY: build
build: build-cross

# usage: make clean build-cross dist VERSION=v0.2-alpha
.PHONY: build-cross
build-cross: LDFLAGS += -extldflags "-static"
build-cross: bootstrap-gox
	CGO_ENABLED=0 gox -parallel=3 -output="_dist/{{.OS}}-{{.Arch}}/avro-kedavro-checker" -osarch='$(TARGETS)' -ldflags '$(LDFLAGS)' $(BUILD_PATH)

.PHONY: bootstrap-gox
bootstrap-gox: 
	@echo "bootstrap-gox target..."
ifndef HAS_GOX
	@go get -u github.com/mitchellh/gox
endif

.PHONY: dist
dist: build
	ls
	@( \
		cd _dist && \
		$(DIST_DIRS) tar -zcf $(NAME)-${VERSION}-{}.tar.gz {} \; && \
		$(DIST_DIRS) zip -r $(NAME)-${VERSION}-{}.zip {} \; \
	)

.PHONY: clean
clean:
	@rm -rf $(BINDIR) ./_dist