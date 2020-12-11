DEPLOYMENT_NAME=$1

yq w -i k8s-peer.yaml "metadata.name" $DEPLOYMENT_NAME
yq w -i k8s-peer.yaml "metadata.labels.app" $DEPLOYMENT_NAME 
yq w -i k8s-peer.yaml "spec.selector.matchLabels.app" $DEPLOYMENT_NAME 
yq w -i k8s-peer.yaml "spec.template.metadata.labels.app" $DEPLOYMENT_NAME


yq w -i k8s-peer-service.yaml "metadata.name" $DEPLOYMENT_NAME 
yq w -i k8s-peer-service.yaml "metadata.labels.run" $DEPLOYMENT_NAME 
yq w -i k8s-peer-service.yaml "spec.selector.app" $DEPLOYMENT_NAME
yq w  -i k8s-peer.yaml 'spec.template.spec.containers(name==peer0-org1-scray-org).env(name==CORE_PEER_ID).valueFrom.configMapKeyRef.name' ff
yq w  -i k8s-peer.yaml 'spec.template.spec.containers(name==peer0-org1-scray-org).env(name==CORE_PEER_ADDRESS).valueFrom.configMapKeyRef.name' ff
yq w  -i k8s-peer.yaml 'spec.template.spec.containers(name==peer0-org1-scray-org).env(name==CORE_PEER_CHAINCODEADDRESS).valueFrom.configMapKeyRef.name' ff
yq w  -i k8s-peer.yaml 'spec.template.spec.containers(name==peer0-org1-scray-org).env(name==CORE_PEER_GOSSIP_BOOTSTRAP).valueFrom.configMapKeyRef.name' ff
yq w  -i k8s-peer.yaml 'spec.template.spec.containers(name==peer0-org1-scray-org).env(name==CORE_PEER_GOSSIP_EXTERNALENDPOINT).valueFrom.configMapKeyRef.name' ff
yq w  -i k8s-peer.yaml 'spec.template.spec.containers(name==peer0-org1-scray-org).env(name==CORE_PEER_LOCALMSPID).valueFrom.configMapKeyRef.name' ff
