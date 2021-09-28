# Application to interact with Blockchain

Write new asset to blockchain and read all assets.

### Update connection description
Prerequisites
``` 
PEER_NAME=peer48
PEER_HOST_NAME=$PEER_NAME.kubernetes.research.dev.seeburger.de 
```
* Example to get ca path
    ```
    cat /mnt/conf/organizations/peerOrganizations/$PEER_HOST_NAME/tlsca/tlsca.$PEER_HOST_NAME-cert.pem
    ```

* Get peer port
   ```
   GOSSIP_PORT=$(kubectl get service $PEER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-listen')].nodePort}")
   ```
  
### Create wallet
A description can be found [here](../../tools/wallet-creator/).
