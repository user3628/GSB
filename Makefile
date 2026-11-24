.PHONY: build test race vet windows demo

build:
	go build -mod=vendor -o rewardpack ./cmd/rewardpack

test:
	go test -mod=vendor ./...

race:
	go test -mod=vendor -race ./...

vet:
	go vet -mod=vendor ./...

windows:
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -mod=vendor ./...

demo: build
	@dir=$$(mktemp -d); \
	./rewardpack build --db $$dir/reward.sqlite --config sample/config.yaml --shards sample/shards --out $$dir/artifacts; \
	./rewardpack verify --db $$dir/reward.sqlite; \
	echo "demo artifacts: $$dir/artifacts"
