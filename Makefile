.PHONY: build test bench vet race

build:
	go build -trimpath -o bin/gdrive2s3 ./cmd/gdrive2s3

test:
	go test ./...

vet:
	go vet ./...

race:
	go test -race ./...

bench:
	go test ./... -run '^$' -bench . -benchmem
