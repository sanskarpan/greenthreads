GO ?= go
GO_VERSION ?= 1.26.5
COVERAGE_MIN ?= 80

.PHONY: build test race lint vet vuln coverage fuzz bench check docker

build:
	$(GO) build -trimpath ./...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

lint:
	golangci-lint run ./...

vet:
	$(GO) vet ./...

vuln:
	govulncheck ./...

coverage:
	$(GO) test -coverprofile=coverage.out ./...
	@total=$$($(GO) tool cover -func=coverage.out | awk '/total:/ {gsub("%", "", $$3); print $$3}'); \
	awk -v total="$$total" -v minimum="$(COVERAGE_MIN)" 'BEGIN { if (total < minimum) { printf "coverage %.1f%% is below %.1f%%\n", total, minimum; exit 1 } }'

fuzz:
	$(GO) test -run=^$$ -fuzz=FuzzStackPopNeverPanics -fuzztime=10s ./internal/fiber
	$(GO) test -run=^$$ -fuzz=FuzzPayloadIntNeverPanics -fuzztime=10s ./web
	$(GO) test -run=^$$ -fuzz=FuzzPayloadStringNeverPanics -fuzztime=10s ./web

bench:
	$(GO) test -run=^$$ -bench=. -benchmem ./internal/...

check: build test race vet lint coverage

docker:
	docker build --tag greenthreads:local .
