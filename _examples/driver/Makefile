BINARY = gleam
SOURCE_DIR = github.com/chrislusf/gleam/distributed/gleam

BINARY_DRIVER = driver
DRIVER_SOURCE_DIR = github.com/eiso/go-engine/_examples/driver

BINARY_MULTITOOL = multitool

.PHONY : clean deps binaries docker run_docker

clean:
	go clean $(SOURCE_DIR)
	go clean $(DRIVER_SOURCE_DIR)
	rm -f $(BINARY)
	rm -f $(BINARY_DRIVER)
	rm -f $(BINARY_DRIVER)-darwin
	rm -f $(BINARY_MULTITOOL)

deps:
	go get $(GO_FLAGS) -d $(SOURCE_DIR)
	go get $(GO_FLAGS) -d $(DRIVER_SOURCE_DIR)
	dep ensure
	cp -R $(GOPATH)/src/github.com/chrislusf/gleam/distributed/ ../../vendor/github.com/chrislusf/gleam/

binaries: deps
	CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o $(BINARY) $(SOURCE_DIR)
	CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o $(BINARY_DRIVER) $(DRIVER_SOURCE_DIR)
	wget https://github.com/src-d/datasets/releases/download/v2/multitool.linux_amd64.gz
	gunzip multitool.linux_amd64.gz
	mv multitool.linux_amd64 $(BINARY_MULTITOOL)
	chmod +x $(BINARY_MULTITOOL) 

binaries-darwin: deps
	GOOS=darwin CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o $(BINARY_DRIVER)-darwin $(DRIVER_SOURCE_DIR)

docker: binaries
	docker build -f docker/Dockerfile.driver -t eisokant/driver .
	docker push eisokant/driver
	docker build -f docker/Dockerfile.gleam -t eisokant/gleam .
	docker push eisokant/gleam
	docker build -f docker/Dockerfile.multitool -t eisokant/multitool .
	docker push eisokant/multitool

run_docker: docker
	docker-compose up