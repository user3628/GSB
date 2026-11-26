.PHONY: build test race run smoke clean

build:
	./scripts/build.sh

test:
	go test ./... -count=1 -timeout=120s

race:
	go test -race ./internal/... -count=1 -timeout=180s

run:
	./scripts/run.sh

smoke:
	./scripts/smoke.sh

clean:
	rm -rf bin data authserver.log dnscache.log
