## Create user wallet for a given CA

### Requirements
  * MAVEN
  * openssl  
  
### Create wallet
```
CA_CERT=~/libs/fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/ca/ca.org1.example.com-cert.pem
CA_KEY=~/libs/fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/ca/priv_sk
USER=Alice
./cert-creator.sh --cacert $CA_CERT --cakey $CA_KEY --new-user-crt $USER --organizational-unit admin --create-wallet
```

Wallets are stored in ./wallet
