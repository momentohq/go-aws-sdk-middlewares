.PHONY: install-devtools
install-devtools:
	go install golang.org/x/tools/cmd/goimports@v0.25.0
	go install honnef.co/go/tools/cmd/staticcheck@v0.5.1
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
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

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
