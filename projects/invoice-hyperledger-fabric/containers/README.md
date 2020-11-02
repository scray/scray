## Start new peer
```kubectl apply -f k8s-test-network-peer0.yaml```

* Connect to CLI
    ```kubectl exec --stdin --tty $POD_NAME -c scray-peer-cli -- /bin/bash```

* Follow logs of peer
    ```kubectl logs -f  $POD_NAME -c peer0-org1-scray-org```


## 

´´´docker build -t scrayorg/start-hl-fabric-test-network .´´´

´´´
docker run -e "DOCKER_HOST=tcp://192.168.1.44:2375" -it scrayorg/start-hl-fabric-test-network /bin/bash

´´´
	/lib/systemd/system/docker.service
	-H fd:// -H tcp://192.168.1.44:2375
´´´
        command: ['/bin/sh', '-c']
        args: ['cd /mnt/config && git clone --single-branch -b master https://github.com/scray/scray.git && chmod -R a+rw ./scray']




```
curl --user 'scray:scray'  hl-fabric-data-share-service:30080
```

## Add new peer to channel
  *[Tutorial](https://hyperledger-fabric.readthedocs.io/en/latest/channel_update_tutorial.htmlhttps://hyperledger-fabric.readthedocs.io/en/latest/channel_update_tutorial.html)

# Upload config of new org 
curl --user 'scray:scray' -X MKCOL 'http://10.14.128.38:30080/add_requests'
curl --user 'scray:scray' -X MKCOL 'http://10.14.128.38:30080/add_requests/${channelname}'
curl --user 'scray:scray' -T  '/home/ubuntu/git/fabric-samples/test-network/organizations/peerOrganizations/org3.example.com/org3.json' 'http://10.14.128.38:30080/add_requests/mychannel/org3.json'


docker build -t scrayorg:hl-fabric-peer-crypto-generator . && docker run scrayorg:hl-fabric-peer-crypto-generator

# Add new org to config

curl --user 'scray:scray' http://10.15.136.41:30080/add_requests/mychannel/org3.json > org3.json

# Add new node
	## Update etc hosts
	10.14.128.30  orderer.example.com

export CORE_PEER_MSPCONFIGPATH=/tmp/conf/organizations/peerOrganizations/org3.example.com/users/Admin@org3.example.com/msp
export CORE_PEER_TLS_ROOTCERT_FILE=/tmp/conf/organizations/peerOrganizations/org3.example.com/peers/peer0.org3.example.com/tls/ca.crt
export CORE_PEER_ADDRESS=peer0.org3.example.com:30001



peer channel fetch 0 mychannel.block -o orderer.example.com:7050 -c $CHANNEL_NAME --tls --cafile $ORDERER_CA
peer channel join -b mychannel.block
p
	
	
	kubectl exec --stdin --tty peer0-org1-scray-org-6967c9744b-dt95n  -c scray-peer-cli -- /bin/sh
	docker exec Org3cli ./scripts/org3-scripts/step1org3.sh $CHANNEL_NAME $CLI_DELAY $CLI_TIMEOUT $VERBOSE
	
