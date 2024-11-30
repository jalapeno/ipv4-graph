REGISTRY_NAME?=docker.io/iejalapeno
IMAGE_VERSION?=latest

.PHONY: all ipv4-graph container push clean test

ifdef V
TESTARGS = -v -args -alsologtostderr -v 5
else
TESTARGS =
endif

all: ipv4-graph

ipv4-graph:
	mkdir -p bin
	$(MAKE) -C ./cmd compile-ipv4-graph

ipv4-graph-container: ipv4-graph
	docker build -t $(REGISTRY_NAME)/ipv4-graph:$(IMAGE_VERSION) -f ./build/Dockerfile.ipv4-graph .

push: ipv4-graph-container
	docker push $(REGISTRY_NAME)/ipv4-graph:$(IMAGE_VERSION)

clean:
	rm -rf bin

test:
	GO111MODULE=on go test `go list ./... | grep -v 'vendor'` $(TESTARGS)
	GO111MODULE=on go vet `go list ./... | grep -v vendor`
