.PHONY: \
	install-deps \
	gen \

install-deps: ## Install dependencies
	go mod tidy
	go install go.uber.org/mock/mockgen@latest
	go install golang.org/x/tools/cmd/stringer@latest

gen: ## Execute Go generate commands to process source code annotations
	find ./ -type d -name "mock_*" -exec rm -r {} +
	go generate ./...
