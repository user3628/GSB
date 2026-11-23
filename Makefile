.PHONY: build test vet sample server experiment

build:
	go build ./...

test:
	go test ./... -count=1

vet:
	go vet ./...

sample:
	go run ./cmd/samplegen -out data

server:
	go run ./cmd/server -addr 127.0.0.1:$${PORT:-18080} -db $${SHUFFLE_DB:-data/engine.db}

experiment:
	go run ./cmd/experiment -workdir artifacts/experiment -clean
