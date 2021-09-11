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

## Create on cli container


```
apk add openssl
apk add openjdk8

CA_CERT=/mnt/conf/organizations/peerOrganizations/$HOSTNAME/ca/ca.*.pem
CA_KEY=/mnt/conf/organizations/peerOrganizations/$HOSTNAME/ca/priv_sk
USER=Alice
ORG_NAME=peer50
./cert-creator.sh \
--cacert $CA_CERT --cakey $CA_KEY \
--new-user-crt $USER \
--organizational-unit admin \
--create-wallet true \
--org $ORG_NAME \
--wallet-creator-lib-path target
```
## Example workflow
### App side
* ```./cert-creator.sh create_csr --common-name otto```
* ```./cert-creator.sh push_csr --common-name otto --shared-fs-host kubernetes.research.dev.seeburger.de:30080```
* GOTO Peer side
* ```./cert-creator.sh pull_signed_crt --common-name otto --shared-fs-host kubernetes.research.dev.seeburger.de:30080```
* ```./cert-creator.sh create_wallet --common-name otto --mspId peer2MSP``` 


### Peer side
```kubectl exec --stdin --tty $PEER_POD -c scray-peer-cli -- /bin/sh```
* ```./cert-creator.sh pull_csr --common-name otto --shared-fs-host kubernetes.research.dev.seeburger.de:30080```

* ```
  CA_CERT=/mnt/conf/organizations/peerOrganizations/$HOSTNAME/ca/ca.*.pem
  CA_KEY=/mnt/conf/organizations/peerOrganizations/$HOSTNAME/ca/priv_sk
  
  ./cert-creator.sh sign_csr --common-name otto --cacert $CA_CERT --cakey $CA_KEY
   ```


* ```
  ./cert-creator.sh push_crt --common-name otto --shared-fs-host kubernetes.research.dev.seeburger.de:30080
  ````
* GOTO App side  


### Connection description

```
 cat /mnt/conf/organizations/peerOrganizations/kubernetes.research.dev.seeburger.de/tlsca/tlsca.kubernetes.research.dev.seeburger.de-cert.pem
GOSSIP_PORT=$(kubectl get service $PEER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-listen')].nodePort}")
```
