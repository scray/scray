#!/bin/bash

echo "$DOCKER_TOKEN" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker build -t scrayorg/mongodb-example-rest . 
docker push scrayorg/mongodb-example-rest
