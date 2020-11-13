# Invoice contract sample

## Prerequisites
[Windows prequists](#windows-prequists)   
Details of the prequisits can be found [here](Prerequisites)

## Bring up Blockchain environment

```
curl -sSL https://raw.githubusercontent.com/scray/scray/feature/contractDockUpdate/projects/invoice-hyperledger-fabric/example-network/bootstrap.sh | bash -s
```

## Interacting with the network

```
cd fabric-samples/test-network
export PATH=${PWD}/../bin:$PATH
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_LOCALMSPID="Org1MSP"
export CORE_PEER_TLS_ROOTCERT_FILE=${PWD}/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt
export CORE_PEER_MSPCONFIGPATH=${PWD}/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp
export CORE_PEER_ADDRESS=localhost:7051

export FABRIC_CFG_PATH=$PWD/../config/
```

### Add new invoice
```
peer chaincode invoke -o localhost:7050 --ordererTLSHostnameOverride orderer.example.com --tls --cafile ${PWD}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem -C mychannel -n scray-invoice-example --peerAddresses localhost:7051 --tlsRootCertFiles ${PWD}/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt --peerAddresses localhost:9051 --tlsRootCertFiles ${PWD}/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt  -c '{"Args":["createInvoice", "k1", "1234", "true", "false"]}'
```

### Query invoice
```
peer chaincode query -C mychannel -n scray-invoice-example -c '{"Args":["queryInvoice", "k1"]}'
```

### Shutdown network
Execute in folder ```fabric-samples/test-network``` command ```./test-network down```


### Windows prequests

* Install [Git](https://git-scm.com/downloads)
* Install [Docker Desktop](https://www.docker.com/products/docker-desktop)

* Add file sharing resource to docker  
    Settings|Resources|FILE SHARING
* Open GIT Bash 
  * switch to sharing resource folder
  * execute commands from [Bring up Blockchain environment](#bring-up-blockchain-environment)

This example is technically based on [Hyperleder fabcar example](https://hyperledger-fabric.readthedocs.io/en/release-2.0/test_network.html)
