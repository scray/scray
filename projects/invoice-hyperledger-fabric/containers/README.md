## Start new peer
Execute in Kubernetes cluster:
  * ```kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/k8s-peer/projects/invoice-hyperledger-fabric/containers/k8s-test-network-peer0.yaml```
  
## Inform example network nodes about new peer
Execute on example-network node:
  * ```docker-compose -f scray/projects/invoice-hyperledger-fabric/containers/docker-compose-test-network-cli.yaml  up -d```
  * ```docker exec  test-network-cli /bin/bash /opt/scray/scripts/inform_existing_nodes.sh $ORDERER_IP```
  
## Add new peer
Execute in Kubernetes cluster:
  * ```kubectl exec --stdin --tty $POD_NAME  -c scray-peer-cli -- /bin/sh /mnt/conf/peer_join.sh $ORDERER_IP  $PEER_IP```

## Create configuration
  *```kubectl create configmap hl-fabric-peer \
    --from-literal=hostname=kubernetes.research.dev.seeburger.de \
    --from-literal=org_name=OrgScray22 \
    --from-literal=CORE_PEER_ADDRESS=peer0.kubernetes.research.dev.seeburger.de:30002 \
    --from-literal=CORE_PEER_GOSSIP_EXTERNALENDPOINT=kubernetes.research.dev.seeburger.de:30001 \
    --from-literal=CORE_PEER_LOCALMSPID=OrgScray22MSP
   ```  	
