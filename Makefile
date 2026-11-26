FUZZTIME ?= 30s

test:
	go test -race -coverpkg=./... -coverprofile=coverage.out -covermode=atomic ./...

lint:
	golangci-lint run --enable misspell

security:
	gosec -quiet ./...
	govulncheck ./...

# Every fuzz target, FUZZTIME each.
fuzz:
	for pkg in . ./fs ./registry; do \
		for f in $$(grep -ho '^func Fuzz[A-Za-z]*' $$pkg/*_test.go | cut -c6-); do \
			./scripts/fuzz.sh $$pkg $$f $(FUZZTIME) || exit 1; \
		done; \
	done

test/coverage:
	go tool cover -html=coverage.out

.PHONY: test lint security fuzz test/coverage
