.PHONY: all build test test-verbose test-race test-cover bench lint fmt vet clean check help qemu-test fuzz fuzz-quick fuzz-medium fuzz-full test-all profile-cpu profile-mem profile-all profile-trace profile-block

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

## fuzz: Run all fuzz tests for 30s each (Go 1.18+)
fuzz:
	$(GOTEST) -fuzz=FuzzParseHeader -fuzztime=30s ./...
	$(GOTEST) -fuzz=FuzzL2Entry -fuzztime=30s ./...
	$(GOTEST) -fuzz=FuzzRefcountEntry -fuzztime=30s ./...

## fuzz-quick: Quick fuzz tests (1 minute each, suitable for CI)
fuzz-quick:
	$(GOTEST) -fuzz=FuzzParseHeader -fuzztime=1m ./...
	$(GOTEST) -fuzz=FuzzL2Entry -fuzztime=1m ./...

## fuzz-medium: Medium fuzz tests (10 minutes each, suitable for PR merge)
fuzz-medium:
	$(GOTEST) -fuzz=FuzzParseHeader -fuzztime=10m ./...
	$(GOTEST) -fuzz=FuzzL2Entry -fuzztime=10m ./...
	$(GOTEST) -fuzz=FuzzRefcountEntry -fuzztime=10m ./...
	$(GOTEST) -fuzz=FuzzReadWrite -fuzztime=10m ./...

## fuzz-full: Full fuzz tests (1 hour each, suitable for nightly builds)
fuzz-full:
	$(GOTEST) -fuzz=FuzzParseHeader -fuzztime=1h ./...
	$(GOTEST) -fuzz=FuzzL2Entry -fuzztime=1h ./...
	$(GOTEST) -fuzz=FuzzRefcountEntry -fuzztime=1h ./...
	$(GOTEST) -fuzz=FuzzReadWrite -fuzztime=1h ./...
	$(GOTEST) -fuzz=FuzzFullImage -fuzztime=1h ./...

## test-all: Run all tests including QEMU interop (requires qemu-img)
test-all: test test-race qemu-test

## install-tools: Install development tools
install-tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

## profile-cpu: Run benchmarks with CPU profiling
profile-cpu:
	@mkdir -p profiles
	$(GOTEST) -bench=. -benchmem -cpuprofile=profiles/cpu.prof ./...
	@echo "CPU profile written to profiles/cpu.prof"
	@echo "View with: go tool pprof -http=:8080 profiles/cpu.prof"

## profile-mem: Run benchmarks with memory profiling
profile-mem:
	@mkdir -p profiles
	$(GOTEST) -bench=. -benchmem -memprofile=profiles/mem.prof ./...
	@echo "Memory profile written to profiles/mem.prof"
	@echo "View with: go tool pprof -http=:8080 profiles/mem.prof"

## profile-all: Run benchmarks with CPU and memory profiling
profile-all:
	@mkdir -p profiles
	$(GOTEST) -bench=. -benchmem -cpuprofile=profiles/cpu.prof -memprofile=profiles/mem.prof ./...
	@echo "Profiles written to profiles/"
	@echo "View CPU:    go tool pprof -http=:8080 profiles/cpu.prof"
	@echo "View Memory: go tool pprof -http=:8081 profiles/mem.prof"

## profile-trace: Run with execution tracer
profile-trace:
	@mkdir -p profiles
	$(GOTEST) -bench=. -trace=profiles/trace.out ./...
	@echo "Trace written to profiles/trace.out"
	@echo "View with: go tool trace profiles/trace.out"

## profile-block: Run with block profiling (shows where goroutines block)
profile-block:
	@mkdir -p profiles
	$(GOTEST) -bench=. -blockprofile=profiles/block.prof ./...
	@echo "Block profile written to profiles/block.prof"
	@echo "View with: go tool pprof -http=:8080 profiles/block.prof"

## help: Show this help
help:
	@echo "go-qcow2 Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':'
