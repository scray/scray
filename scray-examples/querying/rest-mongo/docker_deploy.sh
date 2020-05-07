#!/bin/bash

echo "$DOCKER_TOKEN" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker build -t scrayorg/mongodb-rest-access . 
docker push scrayorg/mongodb-rest-access
