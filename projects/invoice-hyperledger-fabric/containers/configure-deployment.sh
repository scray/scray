#!/bin/bash

PEER_NAME=peer0.scray.org

createEnvForNewConf() {
  mkdir -p target/$PEER_NAME
  cp k8s-peer.yaml ./target/$PEER_NAME
  cp k8s-peer-service.yaml ./target/$PEER_NAME
  cp configure-deployment.sh ./target/$PEER_NAME/
  cd ./target/$PEER_NAME/
}


setValuesInLocalFile() {
  yq w -i k8s-peer.yaml "metadata.name" $PEER_NAME
  yq w -i k8s-peer.yaml "metadata.labels.app" $PEER_NAME 
  yq w -i k8s-peer.yaml "spec.selector.matchLabels.app" $PEER_NAME 
  yq w -i k8s-peer.yaml "spec.template.metadata.labels.app" $PEER_NAME
  yq w -i k8s-peer.yaml "spec.template.spec.containers(name==peer0-org1-scray-org).name" $PEER_NAME

  yq w  -i k8s-peer.yaml "spec.template.spec.containers(name==$PEER_NAME).env(name==CORE_PEER_ID).valueFrom.configMapKeyRef.name" hl-fabric-peer-$PEER_NAME 
  yq w  -i k8s-peer.yaml "spec.template.spec.containers(name==$PEER_NAME).env(name==CORE_PEER_ADDRESS).valueFrom.configMapKeyRef.name" hl-fabric-peer-$PEER_NAME 
  yq w  -i k8s-peer.yaml "spec.template.spec.containers(name==$PEER_NAME).env(name==CORE_PEER_CHAINCODEADDRESS).valueFrom.configMapKeyRef.name" hl-fabric-peer-$PEER_NAME 
  yq w  -i k8s-peer.yaml "spec.template.spec.containers(name==$PEER_NAME).env(name==CORE_PEER_GOSSIP_BOOTSTRAP).valueFrom.configMapKeyRef.name" hl-fabric-peer-$PEER_NAME 
  yq w  -i k8s-peer.yaml "spec.template.spec.containers(name==$PEER_NAME).env(name==CORE_PEER_GOSSIP_EXTERNALENDPOINT).valueFrom.configMapKeyRef.name" hl-fabric-peer-$PEER_NAME 
  yq w  -i k8s-peer.yaml "spec.template.spec.containers(name==$PEER_NAME).env(name==CORE_PEER_LOCALMSPID).valueFrom.configMapKeyRef.name" hl-fabric-peer-$PEER_NAME 

  # Configure service
  yq w -i k8s-peer-service.yaml 'metadata.name' $PEER_NAME 
  yq w -i k8s-peer-service.yaml 'metadata.labels.run' $PEER_NAME 
  yq w -i k8s-peer-service.yaml 'spec.selector.app' $PEER_NAME 
}


usage()
{
    echo "usage: Create peer K8s configuration [[[-n ] [-i]] | [-h]]"
}


while [ "$1" != "" ]; do
    case $1 in
        -n | --name )   shift
				PEER_NAME=$1
				createEnvForNewConf
				setValuesInLocalFile
                                ;;
        -i | --inplace )   	shift
	       			PEER_NAME=$1
				setValuesInLocalFile
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done
