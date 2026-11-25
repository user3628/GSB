.PHONY: build test demo verify clean

build:
	go build -o artifacts/lease-arbiter-bin ./cmd/lease-arbiter

test:
	go test ./... -count=1

demo:
	go run ./cmd/lease-arbiter demo --db artifacts/demo.db

verify:
	./scripts/verify.sh

clean:
	rm -f artifacts/lease-arbiter-bin artifacts/demo.db artifacts/demo.db-wal artifacts/demo.db-shm
	rm -rf artifacts/verification

