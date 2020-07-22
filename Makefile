### Go
build-plugin:
	go build -buildmode=c-shared -o bin/out_gpubsub.so .

build-img:
	docker build -t flb-out_gcloud_pubsub:latest -f examples/Dockerfile .

run:
	docker run -p 24224:24224 --rm flb-out_gcloud_pubsub:latest
