## Create connection description

```
cat /mnt/conf/organizations/peerOrganizations/peer2.kubernetes.research.dev.seeburger.de/tlsca/tlsca.peer2.kubernetes.research.dev.seeburger.de-cert.pem
```
Put it in connection-org1.yaml

## Get peer port
```GOSSIP_PORT=$(kubectl get service $PEER_NAME -o jsonpath="{.spec.ports[?(@.name=='peer-listen')].nodePort}")```
