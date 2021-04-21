#!/bin/bash

REPO_URL=$1
VERISON=1.1

pushDockerHub() {
    echo "$DOCKER_TOKEN" | docker login -u "$DOCKER_USERNAME" --password-stdin
    echo "$DOCKER_TOKEN"
    echo "$DOCKER_USERNAME"
    docker build -t  scrayorg/hl-fabric-external-invoice-example:$VERISON . 
    docker push  scrayorg/hl-fabric-external-invoice-example:$VERISON
}

pushLocal() {
    docker build -t $REPO_URL/research/hl-fabric-external-invoice-example:$VERISON .
    docker push $REPO_URL/research/hl-fabric-external-invoice-example:$VERISON
}

# Remove runtime data
cleanUp() {

}

usage() {
    echo "usage: push container to docker registry [[-h push to docker hub ] [-l push to local registry]]"
}

if [[ -z "$1" ]]
then
    usage
fi

while [ "$1" != "" ]; do
    case $1 in
        -l | --local )   shift
                                REPO_URL=$1
                                cleanUp
                                pushLocal
                                ;;
        -h | -docker-hub )      shift
                                cleanUp
                                pushDockerHub
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done
