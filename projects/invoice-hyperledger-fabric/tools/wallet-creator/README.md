## Create user wallet for a given CA

### Requirements
  * MAVEN
  * openssl  
  
### Create wallet for HL Fabric test-network
```
FABRIC_SAMPLE_BASE_PATH=~
CA_CERT=$FABRIC_SAMPLE_BASE_PATH/fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/ca/ca.org1.example.com-cert.pem
CA_KEY=$FABRIC_SAMPLE_BASE_PATH/fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/ca/priv_sk
USER=Alice
ORG_NAME=peer42MSP
./cert-creator.sh \
  --cacert $CA_CERT --cakey $CA_KEY \
  --new-user-crt $USER \
  --organizational-unit admin \
  --create-wallet true \
  --org $ORG_NAME \
  --wallet-creator-lib-path target
```

Wallets are stored in ./wallet  
An example application can be found hier:  
``scray/projects/invoice-hyperledger-fabric/invoice-service/src/main/java/org/scray/projects/hyperledger_fabric/invoice_service/GetAllAssetsApp.java``
