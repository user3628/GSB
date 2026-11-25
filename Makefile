export GOFLAGS := -mod=vendor
export GOPROXY := off
export CGO_ENABLED := 0

.PHONY: all build test race vet sample demo

all: build test

build:
	go build ./...

test:
	go test ./... -count=1

race:
	go test -race ./... -count=1

vet:
	go vet ./...

sample:
	go run ./cmd/windowctl generate -seed 242 -out samples/deterministic.json

demo:
	./scripts/demo.sh
