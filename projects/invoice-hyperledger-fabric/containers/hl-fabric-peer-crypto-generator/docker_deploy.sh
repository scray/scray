#!/bin/bash

REPO_URL=$1

pushDockerHub() {
	echo "$DOCKER_TOKEN" | docker login -u "$DOCKER_USERNAME" --password-stdin
	echo "$DOCKER_TOKEN"
	echo "$DOCKER_USERNAME"
	docker build -t scrayorg/hl-fabric-node-configurator:1.1 . 
	docker push scrayorg/hl-fabric-node-configurator:1.1
}

pushLocal() {

        docker build -t $REPO_URL/research/hl-fabric-node-configurator:0.1 .
        docker push $REPO_URL/research/hl-fabric-node-configurator:0.1

}

# Remove runtime data
cleanUp() {
	 echo "Remove generated configuratios before building docker container"
	./clean.sh
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
        -h | -docker-hub )   	shift
        						cleanUp
								pushDockerHub
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done
