.PHONY: test race build windows-build demo clean

test:
	go test ./...

race:
	go test -race ./...

build:
	go build ./...

windows-build:
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...

demo:
	go run ./cmd/configimpact demo -fresh

clean:
	rm -f artifacts/*.db artifacts/*.db-wal artifacts/*.db-shm
