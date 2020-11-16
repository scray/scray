# Start external chaincode
  ```kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/k8s-peer/projects/invoice-hyperledger-fabric/chaincode/chaincode-external/k8s-external-chaincode.yaml```

# Install external chaincode on peers
  ```docker exec test-network-cli /bin/bash /opt/scray/scripts/install-external-cc-on-examplenetwork.sh $IP_CC_SERVICE $IP_OF_EXAMPLE_NETWORK```
