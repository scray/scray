## Hyperledger Fabric Kubernetes peer

## Prerequisites

```
git clone https://github.com/scray/scray.git
cd scray/projects/invoice-hyperledger-fabric/containers
kubectl apply -f k8s-hl-fabric-data-share.yaml
```

### Create configuration for new peer

```
PEER_NAME=peer48
PEER_HOST_NAME=$PEER_NAME.kubernetes.research.dev.seeburger.de 
EXT_PEER_IP=10.15.136.41

./configure-deployment.sh -n $PEER_NAME
```

### Start service
  ```kubectl apply -f target/$PEER_NAME/k8s-peer-service.yaml```


### Create peer configuration

   ```
   GOSSIP_PORT=$(kubectl get service $PEER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-listen')].nodePort}")
   PEER_LISTEN_PORT=$(kubectl get service $PEER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-listen')].nodePort}")
   PEER_CHAINCODE_PORT=$(kubectl get service $PEER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-chaincode')].nodePort}")
  ```
```
kubectl delete configmap hl-fabric-peer-$PEER_NAME 
kubectl create configmap hl-fabric-peer-$PEER_NAME \
 --from-literal=hostname=$PEER_HOST_NAME \
 --from-literal=org_name=$PEER_NAME \
 --from-literal=data_share=hl-fabric-data-share-service:80 \
 --from-literal=CORE_PEER_ADDRESS=peer0.$PEER_HOST_NAME:$PEER_LISTEN_PORT \
 --from-literal=CORE_PEER_GOSSIP_EXTERNALENDPOINT=peer0.$PEER_HOST_NAME:$GOSSIP_PORT \
 --from-literal=CORE_PEER_LOCALMSPID=${PEER_NAME}MSP
```    

### Start new peer:

  ```kubectl apply -f target/$PEER_NAME/k8s-peer.yaml```
  
  Print peer logs
  ```
  POD_NAME=$(kubectl get pod -l app=$PEER_NAME -o jsonpath="{.items[0].metadata.name}")
  kubectl logs -f $POD_NAME  -c $PEER_NAME
  ```
  
## Integrate new peer to example network
### Example values
  ```
  ORDERER_IP=10.14.128.30 # Internal IP of orderer
  ORDERER_HOSTNAME=orderer.example.com 
  CHANNEL_NAME=mychannel
  ORG_ID=peer42
  ```

### Addorse new peer data:
  ```docker exec test-network-cli /bin/bash /opt/scray/scripts/inform_existing_nodes.sh $ORDERER_IP $CHANNEL_NAME $ORG_ID```
  

## Integrate new peer to Scray K8s network
### Example values
  ```
  ORDERER_IP=$(kubectl get pods  -l app=orderer-org1-scray-org -o jsonpath='{.items[*].status.podIP}')
  ORDERER_HOSTNAME=orderer.example.com 
  ORDERER_PORT=30081
  # ORDERER_PORT=$(kubectl get service orderer-org1-scray-org -o jsonpath="{.spec.ports[?(@.name=='orderer-listen')].nodePort}")
  CHANNEL_NAME=mychannel
  SHARED_FS_HOST=10.14.128.38:30080 
  ```
  

### Addorse new peer data [add to mychannel]:
```
ORDERER_POD=$(kubectl get pod -l app=orderer-org1-scray-org -o jsonpath="{.items[0].metadata.name}")
kubectl exec --stdin --tty $ORDERER_POD -c scray-orderer-cli  -- /bin/sh /mnt/conf/orderer/scripts/inform_existing_nodes.sh $ORDERER_IP $CHANNEL_NAME $PEER_NAME $SHARED_FS_HOST $EXT_PEER_IP $PEER_HOST_NAME
```
  
### Join network
 ```
  PEER_POD_NAME=$(kubectl get pod -l app=$PEER_NAME -o jsonpath="{.items[0].metadata.name}")
  ORDERER_PORT=$(kubectl get service orderer-org1-scray-org -o jsonpath="{.spec.ports[?(@.name=='orderer-listen')].nodePort}")
  ORDERER_PORT=7050
  PEER_PORT=$(kubectl get service $PEER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-listen')].nodePort}")
  kubectl exec --stdin --tty $PEER_POD_NAME  -c scray-peer-cli -- /bin/sh /mnt/conf/peer_join.sh $ORDERER_IP  $ORDERER_HOSTNAME $ORDERER_PORT $CHANNEL_NAME $SHARED_FS_HOST $EXT_PEER_IP
```

# Export data
### Export channel configuration
Channel configuration can be found here after after the export: ```$SHARED_FS_HOST/channel/configuration/$CHANNEL_NAME/config.json```  
```kubectl exec --stdin --tty $ORDERER_POD -c scray-orderer-cli  -- /bin/sh /mnt/conf/orderer/scripts/publish_channel_conf.sh  $CHANNEL_NAME $SHARED_FS_HOST```
