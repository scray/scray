## Create user wallet for a given CA

### Requirements
  * MAVEN
  * openssl  
  
## Example workflow
### App side
```cd scray/projects/invoice-hyperledger-fabric/tools/wallet-creator```
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
