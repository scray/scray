#!/bin/bash

echo "$DOCKER_TOKEN" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker build -t scrayorg/grafana-configurator scray-examples/persistent-traffic-data/monitoring/grafana-configurator
docker push scrayorg/grafana-configurator
