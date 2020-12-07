## Hyperledger Fabric Kubernetes peer

### Create peer configuration
   ```
   kubectl create configmap hl-fabric-peer \
    --from-literal=hostname=kubernetes.research.dev.seeburger.de \
    --from-literal=org_name=OrgScray \
    --from-literal=CORE_PEER_ADDRESS=kubernetes.research.dev.seeburger.de:30003 \
    --from-literal=CORE_PEER_GOSSIP_EXTERNALENDPOINT=kubernetes.research.dev.seeburger.de:30001 \
    --from-literal=CORE_PEER_LOCALMSPID=OrgScrayMSP
   ```  	

### Start new peer:
  ```kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/k8s-peer/projects/invoice-hyperledger-fabric/containers/k8s-peer.yaml```
  
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
 ```kubectl exec --stdin --tty $POD_NAME  -c scray-peer-cli -- /bin/sh /mnt/conf/peer_join.sh $ORDERER_IP  $ORDERER_HOSTNAME $CHANNEL_NAME```

