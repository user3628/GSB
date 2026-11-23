.PHONY: build test race demo run

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

demo:
	go run ./cmd/demo

run:
	go run . -addr 127.0.0.1:8080 -db data/workbench.db -seed samples/demo.json -allow-crash
