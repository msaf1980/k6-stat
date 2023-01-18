all: test build

VERSION ?= $(shell git describe --abbrev=4 --dirty --always --tags)

CLICKHOUSE_VERSION ?= "clickhouse/clickhouse-server:latest"
CLICKHOUSE_CONTAINER ?= "xk6_output_clickhouse"

# K6_STAT_DB_ADDR ?= "http://localhost:8123"
# K6_STAT_DB ?= "default"

DOCKER ?= docker
GO ?= go

SRCS:=$(shell find . -name '*.go' | grep -v 'vendor')

## help: Prints a list of available build targets.
help:
	echo "Usage: make <OPTIONS> ... <TARGETS>"
	echo ""
	echo "Available targets are:"
	echo ''
	sed -n 's/^##//p' ${PWD}/Makefile | column -t -s ':' | sed -e 's/^/ /'
	echo
	echo "Targets run by default are: `sed -n 's/^all: //p' ./Makefile | sed -e 's/ /, /g' | sed -e 's/\(.*\), /\1, and /'`"

## clean: Removes any previously created build artifacts.
clean:
	rm -f ./k6-stat ./k6-stat-cli

build: FORCE
	GO111MODULE=on ${GO} build -ldflags '-X main.BuildVersion=$(VERSION)' ${PWD}/cmd/k6-stat
	GO111MODULE=on ${GO} build -ldflags '-X main.BuildVersion=$(VERSION)' ${PWD}/cmd/k6-stat-cli

## format: Applies Go formatting to code.
format:
	${GO} fmt ./...

## test: Executes any unit tests.
test:
	${GO} test -cover -race ./...

up:
	${DOCKER} run -d --rm --name "${CLICKHOUSE_CONTAINER}" -p 127.0.0.1:8123:8123 ${CLICKHOUSE_VERSION}
down:
	${DOCKER} stop "${CLICKHOUSE_CONTAINER}"

cli:
	${DOCKER} exec -it "${CLICKHOUSE_CONTAINER}" clickhouse-client

logs:
	${DOCKER} exec -it "${CLICKHOUSE_CONTAINER}" tail -40 /var/log/clickhouse-server/clickhouse-server.log

integrations:
	${GO} test -count=1 -tags=test_integration ./...

lint:
	golangci-lint run

FORCE:

.PHONY: build
