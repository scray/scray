## Hyperledger Fabric Kubernetes orderer

### Create configuration for new order


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

### Start new orderer:

  ```kubectl apply -f target/$ORDERER_NAME/k8s-orderer.yaml```
  
### Delete orderer:

   ```kubectl delete -f k8s-orderer.yaml```
  
## Create new channel
### Example values
  ```
  CHANNEL_NAME=mychannel
  ```

### Create channel
  ```
  ORDERER_POD=$(kubectl get pod -l app=orderer-org1-scray-org -o jsonpath="{.items[0].metadata.name}")
  kubectl exec --stdin --tty $ORDERER_POD -c scray-orderer-cli  -- /bin/sh /mnt/conf/orderer/scripts/create_channel.sh $CHANNEL_NAME
  ```