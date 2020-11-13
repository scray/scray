#!/bin/bash

echo "$DOCKER_TOKEN" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker build -t scrayorg/hl-fabric-external-invoice-example . 
docker push scrayorg/hl-fabric-external-invoice-example 
