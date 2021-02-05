#!/bin/bash

REPO_URL=$1

pushDockerHub() {
	echo "$DOCKER_TOKEN" | docker login -u "$DOCKER_USERNAME" --password-stdin
	echo "$DOCKER_TOKEN"
	echo "$DOCKER_USERNAME"
	docker build -t scrayorg/hl-fabric-peer-crypto-generator:1.1 . 
	docker push scrayorg/hl-fabric-peer-crypto-generator:1.1
}

pushLocal() {

        docker build -t $REPO_URL/research/hl-fabric-node-configurator:0.1 .
        docker push $REPO_URL/research/hl-fabric-node-configurator:0.1

}


while [ "$1" != "" ]; do
    case $1 in
        -l | --local )   shift
                                REPO_URL=$1
				pushLocal
                                ;;
        -h | -docker-hub )   	shift
				pushDockerHub
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done
