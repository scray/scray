## Hyperledger Fabric Kubernetes peer

### Create configuration for new peer

  ```
  PEER_NAME=peer-42
  cd ~/git/scray/projects/invoice-hyperledger-fabric/containers
  ./configure-deployment.sh -n $PEER_NAME
  ```

### Start service
  ```kubectl apply -f target/$PEER_NAME/k8s-peer-service.yaml```


### Create peer configuration

   ```
   GOSSIP_PORT=$(kubectl get service $PEER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-gossip')].nodePort}")
   PEER_LISTEN_PORT=$(kubectl get service $PEER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-listen')].nodePort}")
   PEER_CHAINCODE_PORT=$(kubectl get service $PEER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-chaincode')].nodePort}")
   ```

   ```
   kubectl create configmap hl-fabric-peer-$PEER_NAME \
    --from-literal=hostname=kubernetes.research.dev.seeburger.de \
    --from-literal=org_name=$PEER_NAME \
    --from-literal=CORE_PEER_ADDRESS=kubernetes.research.dev.seeburger.de:$PEER_LISTEN_PORT \
    --from-literal=CORE_PEER_GOSSIP_EXTERNALENDPOINT=kubernetes.research.dev.seeburger.de:$GOSSIP_PORT \
    --from-literal=CORE_PEER_LOCALMSPID=${PEER_NAME}MSP
   ```    	

### Start new peer:

  ```kubectl apply -f target/$PEER_NAME/k8s-peer.yaml```
  
## Integrate new peer to example network
### Example values
  ```
  ORDERER_IP=10.14.128.30 
  ORDERER_HOSTNAME=orderer.example.com 
  CHANNEL_NAME=mychannel
  ORG_ID=OrgScray
  ```

### Addorse new peer data:
  ```docker-compose -f scray/projects/invoice-hyperledger-fabric/containers/docker-compose-test-network-cli.yaml  up -d```  
  ```docker exec test-network-cli /bin/bash /opt/scray/scripts/inform_existing_nodes.sh $ORDERER_IP $CHANNEL_NAME $ORG_ID```
  
### Join network
 ```
POD_NAME=$(kubectl get pod -l app=peer0-org1-scray-org -o jsonpath="{.items[0].metadata.name}")
kubectl exec --stdin --tty $POD_NAME  -c scray-peer-cli -- /bin/sh /mnt/conf/peer_join.sh $ORDERER_IP  $ORDERER_HOSTNAME $CHANNEL_NAME
```

