BINARY = gleam
USER = eiso
SOURCE_DIR = github.com/$(USER)/gleam/distributed/gleam

BINARY_DRIVER = driver
DRIVER_SOURCE_DIR = github.com/eiso/go-engine/_examples/driver

PGA_SOURCE_DIR = github.com/src-d/datasets/PublicGitArchive/pga
BINARY_PGA = pga

.PHONY : clean deps binaries docker run_docker

clean:
	go clean $(SOURCE_DIR)
	go clean $(DRIVER_SOURCE_DIR)
	rm -f $(BINARY)
	rm -f $(BINARY_DRIVER)
	rm -f $(BINARY_DRIVER)-darwin
	rm -f $(BINARY_PGA)

deps: 
	go get $(GO_FLAGS) -d $(SOURCE_DIR)
	go get $(GO_FLAGS) -d $(DRIVER_SOURCE_DIR)
	go get $(GO_FLAGS) -d $(PGA_SOURCE_DIR)
	dep ensure
	git --git-dir=$(GOPATH)/src/github.com/$(USER)/gleam/.git checkout improvements 
	cp -R $(GOPATH)/src/github.com/$(USER)/gleam/distributed/ ../../vendor/github.com/chrislusf/gleam/

binaries: deps
	CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o $(BINARY) $(SOURCE_DIR)
	CGO_ENABLED=0 go build -tags norwfs -a -ldflags '-extldflags "-static"' -o $(BINARY_DRIVER) $(DRIVER_SOURCE_DIR)
	CGO_ENABLED=1 go build -a -ldflags '-extldflags "-static"' -o $(BINARY_PGA) $(PGA_SOURCE_DIR)

binaries-darwin: deps
	GOOS=darwin CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o $(BINARY_DRIVER)-darwin $(DRIVER_SOURCE_DIR)

docker: binaries
	docker build --no-cache -f docker/Dockerfile.driver -t eiso/driver .
	docker push eiso/driver
	docker build --no-cache -f docker/Dockerfile.gleam -t eiso/gleam .
	docker push eiso/gleam
	docker build --no-cache -f docker/Dockerfile.pga -t eiso/pga .
	docker push eiso/pga

driver: deps
	CGO_ENABLED=1 go build -a -ldflags '-extldflags "-static"' -o $(BINARY_DRIVER) $(DRIVER_SOURCE_DIR)
	docker build --no-cache -f docker/Dockerfile.driver -t eiso/driver .
	docker push eiso/driver

driver-experimental: deps
	CGO_ENABLED=1 go build -race -a -ldflags '-extldflags "-static"' -o $(BINARY_DRIVER) $(DRIVER_SOURCE_DIR)
	docker build  --no-cache -f docker/Dockerfile.driver-experimental -t eiso/driver-experimental .
	docker push eiso/driver-experimental

run_docker: docker
	docker-compose -f docker/docker-compose.yaml up