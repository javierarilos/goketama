.DEFAULT_GOAL := help

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: clean
clean:
	go clean ./...

.PHONY: build
build:
	go build ./...

.PHONY: test
test: ## Run tests
	go test -v -race ./...

.PHONY: cov
cov: ## test with coverage
	go test -v -coverprofile coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

