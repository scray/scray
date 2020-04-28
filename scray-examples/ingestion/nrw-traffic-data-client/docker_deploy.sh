#!/bin/bash

echo "$DOCKER_TOKEN" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker build -t scrayorg/nrw-traffic-data-client scray-examples/ingestion/nrw-traffic-data-client/ 
docker push scrayorg/nrw-traffic-data-client
