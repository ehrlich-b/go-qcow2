.PHONY: all build test test-verbose test-race test-cover bench lint fmt vet clean check help qemu-test

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOVET=$(GOCMD) vet

# Build info
BINARY_NAME=go-qcow2
COVERAGE_FILE=coverage.out

all: check build test

## build: Build the library
build:
	$(GOBUILD) -v ./...

## test: Run tests
test:
	$(GOTEST) ./...

## test-verbose: Run tests with verbose output
test-verbose:
	$(GOTEST) -v ./...

## test-race: Run tests with race detector
test-race:
	$(GOTEST) -race ./...

## test-cover: Run tests with coverage
test-cover:
	$(GOTEST) -coverprofile=$(COVERAGE_FILE) ./...
	$(GOCMD) tool cover -func=$(COVERAGE_FILE)

## test-cover-html: Run tests and open coverage report in browser
test-cover-html: test-cover
	$(GOCMD) tool cover -html=$(COVERAGE_FILE)

## bench: Run benchmarks
bench:
	$(GOTEST) -bench=. -benchmem ./...

## lint: Run golangci-lint (requires golangci-lint to be installed)
lint:
	@which golangci-lint > /dev/null || (echo "golangci-lint not installed, run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest" && exit 1)
	golangci-lint run

## fmt: Format code
fmt:
	$(GOFMT) -s -w .

## fmt-check: Check if code is formatted
fmt-check:
	@test -z "$$($(GOFMT) -l .)" || (echo "Code is not formatted. Run 'make fmt'" && exit 1)

## vet: Run go vet
vet:
	$(GOVET) ./...

## check: Run all checks (fmt, vet, build, test)
check: fmt-check vet build test

## clean: Clean build artifacts
clean:
	rm -f $(COVERAGE_FILE)
	$(GOCMD) clean -cache -testcache

## deps: Download dependencies
deps:
	$(GOMOD) download
	$(GOMOD) tidy

## deps-update: Update dependencies
deps-update:
	$(GOMOD) get -u ./...
	$(GOMOD) tidy

## qemu-test: Run tests that compare with QEMU (requires qemu-img)
qemu-test:
	@which qemu-img > /dev/null || (echo "qemu-img not installed" && exit 1)
	$(GOTEST) -v -tags=qemu ./...

## qemu-create-test-images: Create test images with qemu-img for manual testing
qemu-create-test-images:
	@mkdir -p testdata
	@which qemu-img > /dev/null || (echo "qemu-img not installed" && exit 1)
	qemu-img create -f qcow2 testdata/test-v3-64k.qcow2 1G
	qemu-img create -f qcow2 -o cluster_size=4K testdata/test-v3-4k.qcow2 1G
	qemu-img create -f qcow2 -o compat=0.10 testdata/test-v2.qcow2 1G
	qemu-img create -f qcow2 -o lazy_refcounts=on testdata/test-lazy.qcow2 1G
	@echo "Created test images in testdata/"

## qemu-check: Check our images with qemu-img
qemu-check:
	@which qemu-img > /dev/null || (echo "qemu-img not installed" && exit 1)
	@for f in testdata/*.qcow2; do \
		echo "Checking $$f..."; \
		qemu-img check $$f; \
	done

## fuzz: Run fuzz tests (Go 1.18+)
fuzz:
	$(GOTEST) -fuzz=FuzzParseHeader -fuzztime=30s ./...

## install-tools: Install development tools
install-tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

## help: Show this help
help:
	@echo "go-qcow2 Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':'
