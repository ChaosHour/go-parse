# Makefile for go-parse


BINDIR=bin

.PHONY: all build clean scan

all: build

build:
	@echo "Building binaries into $(BINDIR)/"
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/go-parse ./cmd
	go build -o $(BINDIR)/go-parse-scan ./cmd/scan

install: build
	@echo "Binaries built at $(BINDIR)/"

clean:
	@echo "Cleaning..."
	@rm -f $(BINDIR)/go-parse $(BINDIR)/go-parse-scan

scan: build
	@echo "Run ./bin/go-parse-scan -scanDir <dir> -detectLarge <threshold> -parallel <N>"
