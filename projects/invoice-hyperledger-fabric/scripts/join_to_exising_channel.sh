export FABRIC_CFG_PATH=$PWD/../config/

# 
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_LOCALMSPID="OrgScrayMSP"
# export CORE_PEER_TLS_ROOTCERT_FILE=${PWD}/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt
export CORE_PEER_MSPCONFIGPATH=/mnt/conf/organizations/peerOrganizations/org1.fabric.hyperledger.projects.scray.org/users/Admin@org1.fabric.hyperledger.projects.scray.org/msp
export CORE_PEER_ADDRESS=localhost:9051

# Add orderer host to hosts file

apk add curl

# Download orderer CA
SHARED_FS_HOST=hl-fabric-data-share-service:30080
SHARED_FS_USER=scray
SHARED_FS_PW=scray
curl --user $SHARED_FS_USER:$SHARED_FS_PW http://$SHARED_FS_HOST/ca/tlsca.example.com-cert.pem > /tmp/tlsca.example.com-cert.pem
export ORDERER_CA=/tmp/tlsca.example.com-cert.pem
export CHANNEL_NAME=mychannel
export CORE_PEER_ADDRESS=peer0.org1.fabric.hyperledger.projects.scray.org:30003

peer channel fetch 0 mychannel.block -o orderer.example.com:7050 -c $CHANNEL_NAME --tls --cafile $ORDERER_CA
peer channel join -b mychannel.block
