PACKAGES=$(shell go list ./...)

all: lint test

init: tools
	GO111MODULE=on go mod vendor

lint: init
	golangci-lint run ./... --enable-all --disable=lll,scopelint --skip-dirs=vendor

test: init
	go test -race -v ./...

tools:
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(GOPATH)/bin v1.14.0

fmt:
	go fmt $(PACKAGES)

.PHONY: help lint test fmt tools
