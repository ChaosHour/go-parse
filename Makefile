# Makefile for go-parse


BINDIR=bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS = -X github.com/ChaosHour/go-parse/pkg/version.value=$(VERSION)

# Pinned so a new staticcheck release cannot turn a green build red on its
# own. govulncheck stays on @latest: it only fails on vulnerabilities the code
# actually calls, and it fetches the vulnerability database at run time, so a
# pinned binary would buy nothing.
STATICCHECK_VERSION ?= v0.8.1

.PHONY: all build install clean scan test fmt fmt-check vet lint vulncheck check

all: build

build:
	@echo "Building binaries into $(BINDIR)/ (version $(VERSION))"
	@mkdir -p $(BINDIR)
	go build -ldflags "$(LDFLAGS)" -o $(BINDIR)/go-parse ./cmd/go-parse
	go build -ldflags "$(LDFLAGS)" -o $(BINDIR)/go-parse-scan ./cmd/go-parse-scan

install:
	@echo "Installing go-parse and go-parse-scan to GOBIN (version $(VERSION))"
	go install -ldflags "$(LDFLAGS)" ./cmd/go-parse ./cmd/go-parse-scan

test:
	go test -race ./...

fmt:
	gofmt -w .

fmt-check:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "Files not gofmt-clean:"; echo "$$unformatted"; exit 1; \
	fi

vet:
	go vet ./...

lint: vet
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# check runs the same gates as CI
check: fmt-check lint vulncheck test

clean:
	@echo "Cleaning..."
	@rm -f $(BINDIR)/go-parse $(BINDIR)/go-parse-scan

scan: build
	@echo "Run ./bin/go-parse-scan -scanDir <dir> -detectLarge <threshold> -parallel <N>"
