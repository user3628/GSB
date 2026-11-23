.PHONY: run test demo build clean

run:
	go run ./cmd/workbench

test:
	go test ./...

demo:
	go run ./cmd/demo -report artifacts/demo-report.json

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -o bin/aegis-workbench ./cmd/workbench
	CGO_ENABLED=0 go build -o bin/rollback-webhook ./cmd/webhook

clean:
	go clean
