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
```

```
kubectl exec --stdin --tty peer0-org1-scray-org-84ddc5757f-glbgn -c scray-peer-cli -- /bin/sh /mnt/conf/install_and_approve_cc.sh $IP_CC_SERVICE $IP_OF_EXAMPLE_NETWORK
```

# Install external chaincode on example network peers 
```
IP_CC_SERVICE=10.14.128.38         # Host where the chaincode is running
IP_OF_EXAMPLE_NETWORK=10.14.128.30 #Host where the example network is running
```
```
docker-compose -f scray/projects/invoice-hyperledger-fabric/containers/docker-compose-test-network-cli.yaml up -d
``` 

```
docker exec test-network-cli /bin/bash /opt/scray/scripts/example_network_install_and_approve_cc.sh $IP_CC_SERVICE $IP_OF_EXAMPLE_NETWORK
```

# Example query
```peer chaincode query -C mychannel -n basic -c '{"function":"ReadAsset","Args":["asset1"]}'```
