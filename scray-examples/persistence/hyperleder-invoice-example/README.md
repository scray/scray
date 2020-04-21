# Invoice contract sample

This example is technically based on (Hyperleder fabcar example)[https://hyperledger-fabric.readthedocs.io/en/release-2.0/test_network.html]

### Get chaincode
    git clone -b feature/hyperledger-example https://github.com/scray/scray.git
    cd scray/scray-examples/persistence/hyperleder-invoice-example/test-network

### Flolow Hyperleder fabcar tutorial
[https://hyperledger-fabric.readthedocs.io/en/release-2.0/test_network.html]

### Deploy chaincode
    ./network.sh deployCC -l java

### List all invoices
    peer chaincode query -C mychannel -n fabcar -c '{"Args":["queryAllInvoices"]}'
 
## Mark invoice CAR0 as received
  ```peer chaincode invoke -o localhost:7050 --ordererTLSHostnameOverride orderer.example.com --tls true --cafile ${PWD}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem -C mychannel -n fabcar --peerAddresses localhost:7051 --tlsRootCertFiles ${PWD}/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt --peerAddresses localhost:9051 --tlsRootCertFiles ${PWD}/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt -c '{"function":"markAsReceived","Args":["CAR0"]}'```
  
## List all invoices
    peer chaincode query -C mychannel -n fabcar -c '{"Args":["queryAllInvoices"]}'
