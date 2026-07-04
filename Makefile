# Makefile for go-parse


BINDIR=bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS = -X github.com/ChaosHour/go-parse/pkg/version.value=$(VERSION)

.PHONY: all build clean scan

all: build

build:
	@echo "Building binaries into $(BINDIR)/ (version $(VERSION))"
	@mkdir -p $(BINDIR)
	go build -ldflags "$(LDFLAGS)" -o $(BINDIR)/go-parse ./cmd/go-parse
	go build -ldflags "$(LDFLAGS)" -o $(BINDIR)/go-parse-scan ./cmd/go-parse-scan

install: build
	@echo "Binaries built at $(BINDIR)/"

clean:
	@echo "Cleaning..."
	@rm -f $(BINDIR)/go-parse $(BINDIR)/go-parse-scan

scan: build
	@echo "Run ./bin/go-parse-scan -scanDir <dir> -detectLarge <threshold> -parallel <N>"
