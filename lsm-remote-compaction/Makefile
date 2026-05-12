.PHONY: all build test lint clean proto run-worker run-node

GOFLAGS := -trimpath
LDFLAGS := -s -w

all: build

build:
	go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o bin/node   ./cmd/node
	go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o bin/worker ./cmd/worker

test:
	go test ./... -race -timeout 60s

lint:
	go vet ./...

clean:
	rm -rf bin/ /tmp/lsm-data /tmp/lsm-worker

# Regenerate gRPC/protobuf code (requires protoc + protoc-gen-go + protoc-gen-go-grpc)
proto:
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/compaction.proto

# Start a worker on port 9090
run-worker:
	./bin/worker --addr :9090 --id worker-1 --workdir /tmp/lsm-worker

# Start a node using the remote worker
run-node:
	./bin/node --dir /tmp/lsm-data --http :8080 --worker-addr :9090 --id node-1 --quorum 1

# Quick smoke-test: write 10 keys, read them back
demo: build
	@./bin/worker --addr :9090 &
	@sleep 0.3
	@./bin/node --dir /tmp/lsm-demo --http :8081 --worker-addr :9090 &
	@sleep 0.3
	@for i in $$(seq 1 10); do \
		curl -sf -X PUT http://localhost:8081/key/key$$i -d "value$$i"; \
	done
	@echo "Reads:"
	@for i in $$(seq 1 10); do \
		echo "key$$i = $$(curl -sf http://localhost:8081/key/key$$i)"; \
	done
	@pkill -f 'bin/node' || true
	@pkill -f 'bin/worker' || true
	@rm -rf /tmp/lsm-demo
