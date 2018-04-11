PACKAGES=$(shell go list ./...)

all: lint test

go-lint:
	go get -u github.com/alecthomas/gometalinter
	gometalinter --install --force

lint: go-lint
	gometalinter \
		--disable-all \
		--enable=gofmt \
		--enable=goimports \
		--enable=golint \
		--enable=misspell \
		--enable=vetshadow \
		--tests \
		./...

test: vendor
	go test -race -v ./...

vendor:
	go get -u -v github.com/kardianos/govendor
	govendor init
	govendor fetch +m

fmt:
	go fmt $(PACKAGES)

.PHONY: help lint test fmt tools
