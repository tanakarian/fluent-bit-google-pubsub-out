### Go
build-plugin:
	go build -buildmode=c-shared -o bin/out_gpubsub.so .

build-img:
	docker build -t flb-out_gcloud_pubsub:latest -f examples/Dockerfile .

run:
	docker run --rm flb-out_gcloud_pubsub:latest
