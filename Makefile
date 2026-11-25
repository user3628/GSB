GO ?= go
DB ?= artifacts/ropechunk-demo.db
BUNDLE ?= artifacts/demo-bundle.json
REPORT ?= artifacts/demo-report.json

.PHONY: test race build demo serve clean-demo acceptance

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

build:
	mkdir -p bin
	$(GO) build -o bin/ropechunk ./cmd/ropechunk

demo:
	$(GO) run ./cmd/ropechunk demo --db $(DB) --bundle $(BUNDLE) --report $(REPORT) --clean

serve:
	$(GO) run ./cmd/ropechunk serve --db $(DB) --addr 127.0.0.1:8080

clean-demo:
	$(GO) run ./cmd/ropechunk clean --db $(DB)

acceptance: test demo
