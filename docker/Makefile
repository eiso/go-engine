BINARY = gleam
SOURCE_DIR = ../distributed/gleam

BINARY_PIPELINE = pipeline
PIPELINE_SOURCE_DIR = ../examples/git

all: gleam

.PHONY : clean deps docker run_docker

clean:
	go clean $(SOURCE_DIR)
	go clean $(PIPELINE_SOURCE_DIR)
	rm -f $(BINARY)
	rm -f $(BINARY_PIPELINE)

deps:
	go get $(GO_FLAGS) -d $(SOURCE_DIR)

gleam: deps
	CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o $(BINARY) $(SOURCE_DIR)

docker: gleam
	docker build -f Dockerfile.gleam -t eisokant/gleam .
	docker push eisokant/gleam
	rm -f $(BINARY)

run_docker: docker
	docker-compose up

deps_pipeline:
	go get $(GO_FLAGS) -d $(PIPELINE_SOURCE_DIR)

pipeline: deps_pipeline
	CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o $(BINARY_PIPELINE) $(PIPELINE_SOURCE_DIR)

docker_pipeline: pipeline
	docker build -f Dockerfile.pipeline -t eisokant/pipeline .
	docker push eisokant/pipeline