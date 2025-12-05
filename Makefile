
IMG ?= ghcr.io/rubenvp8510/perf-test-tempo-opensearch/query-load-generator
VERSION ?= 5
BINARY ?= query-load-generator

all: image-build image-push

build:
	go build -o $(BINARY) ./cmd/query-load-generator

image-build:
	docker build -f Dockerfile -t ${IMG}:${VERSION} .

image-push:
	docker push ${IMG}:${VERSION}

run:
	CONFIG_FILE=config.yaml go run ./cmd/query-load-generator
