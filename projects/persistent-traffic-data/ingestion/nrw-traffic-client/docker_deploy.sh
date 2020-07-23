#!/bin/bash

echo "$DOCKER_TOKEN" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker build -t scrayorg/nrw-traffic-client scray-examples/ingestion/prometheus_crawler/ 
docker push scrayorg/nrw-traffic-client