## Hyperledger Fabric Kubernetes orderer

### Create configuration for new order

  ```
  ORDERER_NAME=Orderer
  cd ~/git/scray/projects/invoice-hyperledger-fabric/containers/orderer/
  ./configure-orderer-deployment.sh -n $ORDERER_NAME
  ```

### Start service
  ```kubectl apply -f target/$ORDERER_NAME/k8s-orderer-service.yaml```


### Create peer configuration

   ```
   GOSSIP_PORT=$(kubectl get service $ORDERER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-gossip')].nodePort}")
   PEER_LISTEN_PORT=$(kubectl get service $ORDERER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-listen')].nodePort}")
   PEER_CHAINCODE_PORT=$(kubectl get service $ORDERER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-chaincode')].nodePort}")
   ```

   ```
   kubectl create configmap hl-fabric-orderer \
    --from-literal=hostname=example.com \
    --from-literal=org_name=$ORDERER_NAME \
    --from-literal=CORE_PEER_ADDRESS=kubernetes.research.dev.seeburger.de:$PEER_LISTEN_PORT \
    --from-literal=CORE_PEER_GOSSIP_EXTERNALENDPOINT=kubernetes.research.dev.seeburger.de:$GOSSIP_PORT \
    --from-literal=ORDERER_GENERAL_LOCALMSPID=${ORDERER_NAME}MSP \
    --from-literal=NODE_TYPE=orderer
   ```

### Start new peer:

  ```kubectl apply -f target/$ORDERER_NAME/k8s-peer.yaml```
  
## Integrate new peer to example network
### Example values
  ```
  ORDERER_IP=10.14.128.30 
  ORDERER_HOSTNAME=orderer.example.com 
  CHANNEL_NAME=mychannel
  ORG_ID=OrgScray
  ```

### Addorse new peer data:
  ```docker exec test-network-cli /bin/bash /opt/scray/scripts/inform_existing_nodes.sh $ORDERER_IP $CHANNEL_NAME $ORG_ID```
  
### Join network
 ```
POD_NAME=$(kubectl get pod -l app=peer0-org1-scray-org -o jsonpath="{.items[0].metadata.name}")
kubectl exec --stdin --tty $POD_NAME  -c scray-peer-cli -- /bin/sh /mnt/conf/peer_join.sh $ORDERER_IP  $ORDERER_HOSTNAME $CHANNEL_NAME
```

## Create new channel
  * Create genensis block from configtx configuration and add it to system channel. For details [read](https://hyperledger-fabric.readthedocs.io/en/release-2.3/create_channel/create_channel.html)
 
    ```docker exec test-network-cli /bin/bash /opt/scray/scripts/create_channel.sh $ORDERER_IP  $CHANNEL_NAME```

  * Join peers to channel
    For the Hyperleder Fabric you can use this tutorial [Link](#Integrate-new-peer-to-example-network)
