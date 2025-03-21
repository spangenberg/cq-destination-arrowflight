.PHONY: test
test:
# we clean the cache to avoid scenarios when we change something in the db and we want to retest without noticing nothing run
	go clean -testcache
	go test -race -timeout 3m ./...

.PHONY: lint
lint:
	golangci-lint run --config ./.golangci.yml

.PHONY: gen-spec-schema
gen-spec-schema:
	go run client/spec/gen/main.go

# All gen targets
.PHONY: gen
gen: gen-spec-schema

.PHONY: build
build:
	go build -o bin/arrowflight .

.PHONY: package
package:
	go run . package -m @CHANGELOG.md ${VERSION} .
