GO ?= go
BIN ?= bin/authlab
OUT ?= artifacts/run

.PHONY: all build test race experiment verify clean

all: test experiment

build:
	$(GO) build -trimpath -o $(BIN) ./cmd/authlab

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

experiment: build
	./$(BIN) experiment --out $(OUT)

verify: build
	./scripts/verify-determinism.sh

clean:
	rm -rf bin artifacts/run/.work
