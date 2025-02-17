all: lint test

lint:
	golangci-lint run ./...

test:
	go test -race -v ./...

fmt:
	go fmt ./...
	golangci-lint run ./... --fix

.PHONY: help lint test fmt
