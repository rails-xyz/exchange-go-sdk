GO ?= go
SPECS := auth account order market options options-matching

# Generated output differs between generator versions, so `make generate` pins one. Install it
# with: go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION)
OAPI_CODEGEN_VERSION := v2.8.0

.PHONY: help generate test vet lint example release

help:
	@echo "  make generate          # regenerate internal/wire/* from api/*.json (committed)"
	@echo "  make test              # unit tests, race detector on, no network"
	@echo "  make vet               # go vet + go.mod tidiness + gofmt check"
	@echo "  make lint              # golangci-lint"
	@echo "  make example           # build everything under examples/"
	@echo "  make release VERSION=x.y.z  # annotated tag vx.y.z; needs a clean tree and a CHANGELOG.md section for it"

generate:
	@have=$$(oapi-codegen -version 2>/dev/null | tail -1); \
	if [ "$$have" != "$(OAPI_CODEGEN_VERSION)" ]; then \
		echo "oapi-codegen $(OAPI_CODEGEN_VERSION) required, found $${have:-none}"; \
		echo "  go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION)"; \
		exit 1; \
	fi
	@for spec in $(SPECS); do \
		echo "oapi-codegen -config api/codegen.$$spec.yaml api/$$spec.json"; \
		oapi-codegen -config api/codegen.$$spec.yaml api/$$spec.json || exit 1; \
	done

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...
	$(GO) mod tidy -diff
	@files=$$(gofmt -s -l .); \
	if [ -n "$$files" ]; then echo "gofmt -s needed:"; echo "$$files"; exit 1; fi

lint:
	golangci-lint run ./...

example:
	@tmp=$$(mktemp -d); $(GO) build -o $$tmp ./examples/...; rc=$$?; rm -rf $$tmp; exit $$rc

# Tags only. Pushing the tag is the release, and stays a deliberate step.
release:
	@test -n "$(VERSION)" || { echo "usage: make release VERSION=x.y.z"; exit 1; }
	@git diff --quiet && git diff --cached --quiet || { echo "the working tree is not clean"; exit 1; }
	@grep -q "^## \[$(VERSION)\]" CHANGELOG.md || { echo "CHANGELOG.md has no ## [$(VERSION)] section: rename [Unreleased], commit"; exit 1; }
	@! git rev-parse -q --verify refs/tags/v$(VERSION) >/dev/null || { echo "v$(VERSION) is already tagged"; exit 1; }
	git tag -a v$(VERSION) -m "v$(VERSION)"
	@echo "tagged v$(VERSION); publish it with: git push origin v$(VERSION)"
