.PHONY: build test race vet demo samples

build:
	go build -o h2diag ./cmd/h2diag

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

demo:
	./scripts/demo.sh

samples:
	go run ./cmd/samplegen -out samples
