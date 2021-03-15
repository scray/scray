## Hyperledger Fabric Kubernetes orderer

### Create configuration for new order

```
ORDERER_NAME=orderer
HOST_NAME=example.com
cd scray/projects/invoice-hyperledger-fabric/containers/orderer
```


### Start service
  ```kubectl apply -f k8s-orderer-service.yaml```


### Create orderer configuration


   ```
   kubectl create configmap hl-fabric-orderer \
    --from-literal=hostname=example.com \
    --from-literal=org_name=$ORDERER_NAME \
    --from-literal=CORE_PEER_ADDRESS=$HOST_NAME:$PEER_LISTEN_PORT \
    --from-literal=CORE_PEER_GOSSIP_EXTERNALENDPOINT=$HOST_NAME:$GOSSIP_PORT \
    --from-literal=ORDERER_GENERAL_LOCALMSPID=${ORDERER_NAME}MSP \
    --from-literal=NODE_TYPE=orderer
   ```

### Start new orderer:

  ```kubectl apply -f k8s-orderer.yaml```
  
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
  kubectl exec --stdin --tty $ORDERER_POD -c scray-orderer-cli  -- /bin/sh /mnt/conf/orderer/scripts/create_channel.sh $CHANNEL_NAME orderer.example.com 7050
  ```