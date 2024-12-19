#!/bin/bash

REPO_URL=$1
VERISON=1.3

pushDockerHub() {
	echo "$DOCKER_TOKEN" | docker login -u "$DOCKER_USERNAME" --password-stdin
	echo "$DOCKER_TOKEN"
	echo "$DOCKER_USERNAME"
    
        cd ../../
	docker build -t scrayorg/hl-fabric-node-configurator:$VERISON -f containers/hl-fabric-node-configurator/Dockerfile  .
	docker push scrayorg/hl-fabric-node-configurator:$VERISON
}

pushLocal() {
        image_name=$1
        image_version=$2

        docker build -t $REPO_URL/$image_name:$VERISON -f Dockerfile .
        docker push $REPO_URL/$image_name:$VERISON
}

importMicrok8s() {

        cd ../../
	docker build -t scrayorg/hl-fabric-node-configurator:$VERISON -f containers/hl-fabric-node-configurator/Dockerfile  .
	docker save scrayorg/hl-fabric-node-configurator:$VERISON  > /tmp/hl-fabric-node-configurator:$VERISON.tar 
	microk8s ctr image import  /tmp/hl-fabric-node-configurator:$VERISON.tar
        rm /tmp/hl-fabric-node-configurator:$VERISON.tar	
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
        
        -m | -microk8s )      shift
                                   cleanUp
                                   importMicrok8s 
												                                ;;

	* )                     usage
                                exit 1
    esac
    shift
done
