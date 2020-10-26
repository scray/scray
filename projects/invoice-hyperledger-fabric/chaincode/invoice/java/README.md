# Invoice contract sample



## Bring up Blockchain environment

```
curl -sSL https://bit.ly/2ysbOFE | bash -s
```
	
```
cd fabric-samples/test-network
./network.sh up createChannel
./network.sh deployCC -ccn scray-invoice-example -ccl java -ccp ~/git/scray/projects/invoice-hyperledger-fabric/chaincode/invoice/java/
```

## Interacting with the network

```
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
peer chaincode query -C mychannel -n scray-invoice-example -c '{"Args":["createInvoice", "k1", "1234", "true", "false"]}'
```

### Query invoice
```
peer chaincode query -C mychannel -n scray-invoice-example -c '{"Args":["queryInvoice", "k1"]}'
```


This example is technically based on [Hyperleder fabcar example](https://hyperledger-fabric.readthedocs.io/en/release-2.0/test_network.html)
