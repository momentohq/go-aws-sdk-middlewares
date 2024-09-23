.PHONY: install-devtools
install-devtools:
	go install golang.org/x/tools/cmd/goimports@latest
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install github.com/onsi/ginkgo/v2/ginkgo@v2.8.1

.PHONY: format
format:
	gofmt -s -w .

.PHONY: imports
imports:
	goimports -l -w .

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: vet
vet:
	go vet ./...

.PHONY: staticcheck
staticcheck:
	staticcheck ./...

.PHONY: lint
lint: format imports tidy vet staticcheck

.PHONY: install-protos-devtools
install-protos-devtools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

.PHONY: build
build:
	go build ./...

.PHONY: precommit
precommit: lint test

.PHONY: test
test:
	ginkgo caching/

.PHONY: vendor
vendor:
	go mod vendor
