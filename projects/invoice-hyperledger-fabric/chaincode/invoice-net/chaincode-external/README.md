# Start external chaincode 
```kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/k8s-peer/projects/invoice-hyperledger-fabric/chaincode/chaincode-external/k8s-external-chaincode.yaml```

## Add hostname of external chincode host to /etc/hosts
```docker exec -it peer0.org1.example.com  /bin/sh -c "echo $IP_CC_SERVICE asset-transfer-basic.org1.example.com >> /etc/hosts"```  

```docker exec -it peer0.org2.example.com  /bin/sh -c "echo $IP_CC_SERVICE asset-transfer-basic.org1.example.com >> /etc/hosts"``` 

```docker exec -it orderer.example.com  /bin/sh -c "echo $IP_CC_SERVICE asset-transfer-basic.org1.example.com >> /etc/hosts"```

# Install external chaincode on k8s peer
```
IP_CC_SERVICE=10.14.128.38         # Host where the chaincode is running
IP_OF_EXAMPLE_NETWORK=10.14.128.30 #Host where the example network is running
PEER_POD=$(kubectl get pod -l app=peer0-org1-scray-org -o jsonpath="{.items[0].metadata.name}")
```

```
kubectl exec --stdin --tty $PEER_POD -c scray-peer-cli -- /bin/sh /mnt/conf/install_and_approve_cc.sh $IP_CC_SERVICE $IP_OF_EXAMPLE_NETWORK
```

# Install external chaincode on example network peers 
```
IP_CC_SERVICE=10.14.128.38         # Host where the chaincode is running
IP_OF_EXAMPLE_NETWORK=10.14.128.30 #Host where the example network is running
```

```
docker exec test-network-cli /bin/bash /opt/scray/scripts/example_network_install_and_approve_cc.sh $IP_CC_SERVICE $IP_OF_EXAMPLE_NETWORK
```
# Commit chaincode
``` docker exec test-network-cli /bin/bash /opt/scray/scripts/example_network_commit_cc.sh $IP_CC_SERVICE $IP_OF_EXAMPLE_NETWORK```

# Example query
```peer chaincode query -C mychannel -n basic -c '{"function":"ReadAsset","Args":["asset1"]}'```
