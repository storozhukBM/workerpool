test: clean
	go test -race -coverprofile coverage.out ./...

coverage: test
	go tool cover -html=coverage.out

bench: install-benchstat
	go test -timeout 3h -count=5 -run=xxx -bench=BenchmarkPoolOverhead ./... | tee stat.txt
	benchstat stat.txt
	benchstat -csv stat.txt > stat.csv

lint: install-golangci-lint
	golangci-lint run

debug-inline:
	go build -gcflags='-m -d=ssa/check_bce/debug=1' ./workerpool.go

clean:
	@go clean
	@rm -f profile.out
	@rm -f coverage.out

install-benchstat:
	@which benchstat || go install golang.org/x/perf/cmd/benchstat@latest

install-golangci-lint:
	@which golangci-lint || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.43.0

help:
	@awk '$$1 ~ /^.*:/ {print substr($$1, 0, length($$1)-1)}' Makefile
