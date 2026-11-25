.PHONY: build test vet offline-test accept demo serve

build:
	go build -o bin/imretrace ./cmd/imretrace

test:
	GOPROXY=off GOFLAGS=-mod=mod go test -count=1 ./...

vet:
	GOPROXY=off GOFLAGS=-mod=mod go vet ./...

offline-test:
	GOPROXY=off GOFLAGS=-mod=mod go test -count=1 ./...

accept:
	./scripts/acceptance.sh

demo: build
	./bin/imretrace prepare
	./bin/imretrace build
	./bin/imretrace search-text --q "red circle 红色圆形 blue gradient" --k 5

serve: build
	./bin/imretrace serve --addr 127.0.0.1:8765
