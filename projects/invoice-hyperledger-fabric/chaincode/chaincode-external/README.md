# Start external chaincode 
```kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/k8s-peer/projects/invoice-hyperledger-fabric/chaincode/chaincode-external/k8s-external-chaincode.yaml```

## Add hostname of external chincode host to /etc/hosts
```docker exec -it peer0.org1.example.com  /bin/sh -c "echo $IP_CC_SERVICE asset-transfer-basic.org1.example.com >> /etc/hosts"```  

```docker exec -it peer0.org2.example.com  /bin/sh -c "echo $IP_CC_SERVICE asset-transfer-basic.org1.example.com >> /etc/hosts"``` 

```docker exec -it orderer.example.com  /bin/sh -c "echo $IP_CC_SERVICE asset-transfer-basic.org1.example.com >> /etc/hosts"```

# Install external chaincode on peers 
```docker-compose -f scray/projects/invoice-hyperledger-fabric/containers/docker-compose-test-network-cli.yaml up -d``` 


```docker exec test-network-cli /bin/bash /opt/scray/scripts/install-external-cc-on-examplenetwork.sh $IP_CC_SERVICE $IP_OF_EXAMPLE_NETWORK```

# Example query
```peer chaincode query -C mychannel -n basic -c '{"function":"ReadAsset","Args":["asset1"]}'```
