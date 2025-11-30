# Dockerfile for go-qcow2 development and testing
# Provides QEMU 8.2+ with full bitmap and LUKS2 support
#
# Build:   docker build -t go-qcow2 .
# Run:     docker run --rm -v $(pwd):/work -w /work go-qcow2 make test
# qemu-img: docker run --rm -v $(pwd):/work -w /work go-qcow2 qemu-img <args>

FROM ubuntu:24.04

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install Go and QEMU tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    qemu-utils \
    qemu-system-common \
    golang-go \
    make \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Verify versions
RUN qemu-img --version && go version

# Set up Go environment
ENV GOPATH=/go
ENV PATH=$PATH:/go/bin

WORKDIR /work
