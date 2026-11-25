GO ?= go

.PHONY: test race vet demo build query serve

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

demo:
	$(GO) run ./cmd/searchlab demo --out demo --seed 42

build:
	$(GO) run ./cmd/searchlab build --config demo/config.yaml --shards demo/shards --db demo/state/searchlab.db

query:
	$(GO) run ./cmd/searchlab query --db demo/state/searchlab.db --terms "Go,语言" --mode AND --query-id demo-and

serve:
	$(GO) run ./cmd/searchlab serve --db demo/state/searchlab.db --config demo/config.yaml --shards demo/shards --addr 127.0.0.1:8080
