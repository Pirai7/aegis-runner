.PHONY: all build run test test-race docker-build clean demo

BINARY_NAME=aegis
GO=$(shell which go 2>/dev/null || echo $(HOME)/.local/go/bin/go)

all: build

build:
	$(GO) build -v -ldflags="-s -w" -o $(BINARY_NAME) ./cmd/aegis

run: build
	./$(BINARY_NAME)

test:
	$(GO) test -v ./...

test-race:
	$(GO) test -race -v ./...

clean:
	rm -f $(BINARY_NAME) aegis-bin

demo:
	python3 scripts/batch_client.py
