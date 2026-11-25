.PHONY: all build test generate demo verify serve serve-demo tidy vendor

all: build

build:
	go build -o bin/csrouter ./cmd/csrouter

test:
	go test ./...

tidy:
	GOSUMDB=off GOPROXY=off go mod tidy

vendor:
	GOSUMDB=off GOPROXY=off go mod vendor

generate: build
	./bin/csrouter generate --root data

demo: build
	./bin/csrouter demo --root data/demo

verify:
	./scripts/verify.sh

serve: build
	./bin/csrouter serve --root data --addr 127.0.0.1:18099

serve-demo: build
	./bin/csrouter serve --root data/demo --addr 127.0.0.1:18099
