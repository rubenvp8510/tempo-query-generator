REPOSITORY ?= rvargasp
IMAGE_NAME ?= query-load-generator
TAG ?= latest
IMG ?= quay.io/${REPOSITORY}/${IMAGE_NAME}:${TAG}
BINARY ?= query-load-generator

all: image-build image-push

build:
	go build -o $(BINARY) ./cmd/query-load-generator

fmt:
	gofmt -w ./cmd ./internal

image-build:
	docker build -f Dockerfile -t ${IMG} .

image-push:
	docker push ${IMG}

run:
	CONFIG_FILE=config.yaml go run ./cmd/query-load-generator
