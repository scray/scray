## Start new peer
  * ```kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/k8s-peer/projects/invoice-hyperledger-fabric/containers/k8s-test-network-peer0.yaml```
  
## Inform example network nodes about new peer
	* ```docker-compose -f scray/projects/invoice-hyperledger-fabric/containers/docker-compose-test-network-cli.yaml  up -d```
	* ```docker exec  Org3cli /bin/bash /opt/scray/scripts/inform_existing_nodes.sh 192.168.55.1```
  
## Add new peer
	* ```kubectl exec --stdin --tty $POD_NAME  -c scray-peer-cli -- /bin/sh /opt/scray/scripts/peer_join.sh $ORDERER_IP  $PEER_IP```
  	