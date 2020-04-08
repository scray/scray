#!/bin/bash

echo "$DOCKER_TOKEN" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker build -t scrayorg/examples:prometheus-crawler scray-examples/ingestion/prometheus_crawler/ 
docker push scrayorg/examples:prometheus-crawler
