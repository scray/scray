#!/bin/bash

echo "$DOCKER_TOKEN" | docker login -u "$DOCKER_USERNAME" --password-stdin
echo "$DOCKER_TOKEN"
echo "$DOCKER_USERNAME"
docker build -t scrayorg/hl-fabric-peer-crypto-generator:1.1 . 
docker push scrayorg/hl-fabric-peer-crypto-generator:1.1 
